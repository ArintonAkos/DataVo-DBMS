using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser;
using DataVo.Core.Runtime;
using DataVo.Core.StorageEngine;
using DataVo.Core.MVCC;

namespace DataVo.Core;

/// <summary>
/// Every row of one table as <see cref="RowRef"/> values, for loops that must not allocate: once a thread has
/// streamed a table of this shape, a full pass allocates zero bytes on the managed heap.
/// </summary>
/// <remarks>
/// <para>
/// Rows are read 1,024 at a time. The read lock is held only while a page is copied out of storage, so writers
/// run between pages and a slow loop never stalls them. A pass sees each row that existed for the whole pass
/// exactly once. A row written or deleted during the pass may or may not be seen.
/// </para>
/// <para>
/// Inside an explicit transaction the stream reads through the query engine so it sees the transaction's own
/// writes; that path allocates. A schema change to the table during a pass stops it with an
/// <see cref="InvalidOperationException"/>.
/// </para>
/// <para>
/// Get one from <see cref="DataVoTable{T}.StreamRef"/> or <see cref="DataVoContext.StreamRef(string)"/>. The pass
/// runs each time the stream is enumerated.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// RowRefStream players = db.Table&lt;Player&gt;().StreamRef();
/// int health = players.GetOrdinal("Health");
/// foreach (RowRef row in players)
/// {
///     total += row.GetFloat(health);
/// }
/// </code>
/// </example>
public readonly struct RowRefStream
{
    private readonly DataVoEngine? _engine;
    private readonly Guid _session;
    private readonly string? _table;

    internal RowRefStream(DataVoEngine engine, Guid session, string table)
    {
        _engine = engine;
        _session = session;
        _table = table;
    }

    /// <summary>Gets the name of the streamed table.</summary>
    public string TableName => _table ?? throw Uninitialized();

    /// <summary>Returns the position of a column, for the <see cref="RowRef"/> accessors.</summary>
    /// <param name="columnName">The column's name; case is ignored.</param>
    /// <returns>The column's position in the table.</returns>
    /// <exception cref="ArgumentException">The table has no such column.</exception>
    /// <remarks>Look positions up once, before the loop: this call allocates.</remarks>
    public int GetOrdinal(string columnName)
    {
        DataVoEngine engine = _engine ?? throw Uninitialized();
        string database = DatabaseOf(engine, _session);
        List<Column> columns = engine.Catalog.GetTableColumns(_table!, database);
        int ordinal = columns.FindIndex(column => string.Equals(column.Name, columnName, StringComparison.OrdinalIgnoreCase));
        return ordinal >= 0
            ? ordinal
            : throw new ArgumentException($"Table {_table} has no column {columnName}.", nameof(columnName));
    }

    /// <summary>Gets the table's current schema version, which changes whenever its columns do.</summary>
    internal int SchemaVersion
    {
        get
        {
            DataVoEngine engine = _engine ?? throw Uninitialized();
            return engine.Catalog.GetTableSchemaVersion(_table!, DatabaseOf(engine, _session));
        }
    }

    /// <summary>
    /// Counts the rows a compiled condition accepts by testing each row inside the storage scan, where it is
    /// stored: no row is copied out, and only the cells up to the last column the condition reads are located.
    /// </summary>
    /// <param name="ordinals">The table positions of the columns the condition reads.</param>
    /// <param name="filter">The condition, already bound to <paramref name="ordinals"/>.</param>
    /// <param name="limit">Stop after this many matches, or 0 to count them all.</param>
    /// <param name="count">The number of matching rows, up to <paramref name="limit"/>.</param>
    /// <returns>
    /// <see langword="false"/> when the rows cannot be read in place (inside a transaction, or while storage holds
    /// uncommitted writes for this flow); the caller then counts through <see cref="GetEnumerator"/>.
    /// </returns>
    /// <remarks>
    /// The read lock is held for one batch of rows at a time, as it is for a streamed pass, so writers run between
    /// batches and the same consistency holds: a row that existed for the whole count is counted exactly once.
    /// </remarks>
    internal bool TryCountInPlace<TFilter>(int[] ordinals, ref TFilter filter, int limit, out int count)
        where TFilter : struct, IRowRefFilter
    {
        count = 0;
        DataVoEngine engine = _engine ?? throw Uninitialized();
        string database = DatabaseOf(engine, _session);
        if (engine.TransactionManager.HasActiveTransaction(_session) || MvccExecutionScope.CurrentSnapshot is not null)
        {
            return false;
        }

        int columns = 0;
        ulong needed = 0;
        foreach (int ordinal in ordinals)
        {
            columns = Math.Max(columns, ordinal + 1);
            if (ordinal < 64)
            {
                needed |= 1UL << ordinal;
            }
        }

        RowRefPage page = RowRefPage.Rent();
        try
        {
            using (engine.EnterRuntimeReadScope())
            {
                page.Open(engine, database, _table!);
            }

            if (columns > page.ColumnCount)
            {
                return false;
            }

            var scan = new InPlaceCount<TFilter>(filter, page, columns, needed, limit);

            // A limited count (Any) starts with a small batch so a match near the start costs little.
            int batch = limit > 0 ? 64 : InPlaceBatchRows;
            while (!page.Cursor.Done && !scan.Full)
            {
                using SnapshotLockScope readScope = engine.EnterRuntimeReadScope();
                if (engine.Catalog.GetTableSchemaVersion(_table!, database) != page.SchemaVersion)
                {
                    throw new InvalidOperationException($"Table {_table} changed shape while it was being counted.");
                }

                if (!engine.StorageContext.ScanRowPage(_table!, database, page.Cursor, null, null, batch, ref scan, InPlaceCount<TFilter>.Visit))
                {
                    return false;
                }

                batch = InPlaceBatchRows;
            }

            count = scan.Count;
            return true;
        }
        finally
        {
            RowRefPage.Return(page);
        }
    }

    // Rows examined per hold of the read lock. Larger than a streamed page: nothing is copied while it is held.
    private const int InPlaceBatchRows = 4096;

    /// <summary>The state of an in-place count: the condition, and the page whose layout locates each row's cells.</summary>
    private struct InPlaceCount<TFilter>(TFilter filter, RowRefPage page, int columns, ulong needed, int limit)
        where TFilter : struct, IRowRefFilter
    {
        public static readonly StorageContext.RawRowVisitor<InPlaceCount<TFilter>> Visit =
            static (ref InPlaceCount<TFilter> scan, RowKey rowId, ReadOnlySpan<byte> stored, StoredRow? typed) =>
            {
                if (scan.Full)
                {
                    return false;
                }

                RowRef row = typed is null
                    ? scan._page.InPlace(rowId, stored, scan._columns)
                    : scan._page.InPlace(rowId, typed, scan._columns, scan._needed);
                if (scan._filter.Matches(in row))
                {
                    scan.Count++;
                }

                return !scan.Full;
            };

        private TFilter _filter = filter;
        private readonly RowRefPage _page = page;
        private readonly int _columns = columns;
        private readonly ulong _needed = needed;
        private readonly int _limit = limit;

        public int Count;

        public readonly bool Full => _limit > 0 && Count >= _limit;
    }

    /// <summary>Starts a pass over the table.</summary>
    /// <returns>An enumerator positioned before the first row.</returns>
    public RowRefEnumerator GetEnumerator()
    {
        DataVoEngine engine = _engine ?? throw Uninitialized();
        return new RowRefEnumerator(engine, _session, DatabaseOf(engine, _session), _table!);
    }

    private static string DatabaseOf(DataVoEngine engine, Guid session) =>
        engine.Sessions.Get(session) is { Length: > 0 } database
            ? database
            : throw new InvalidOperationException("No database selected for the current session. Execute USE <database> first.");

    private static InvalidOperationException Uninitialized() =>
        new("This RowRefStream was not created by StreamRef.");
}

/// <summary>Walks a <see cref="RowRefStream"/>. Use it through <see langword="foreach"/>, which disposes it.</summary>
public ref struct RowRefEnumerator
{
    private static readonly StorageContext.RawRowVisitor<RowRefPage> CopyRow =
        static (ref RowRefPage page, RowKey rowId, ReadOnlySpan<byte> stored, StoredRow? typed) =>
        {
            page.Add(rowId, stored, typed);
            return true;
        };

    private readonly DataVoEngine _engine;
    private readonly string _database;
    private readonly string _table;
    private RowRefPage? _page;
    private IEnumerator<DataVoRow>? _transactional;
    private int _index;

    internal RowRefEnumerator(DataVoEngine engine, Guid session, string database, string table)
    {
        _engine = engine;
        _database = database;
        _table = table;
        _index = -1;
        _transactional = null;
        _page = RowRefPage.Rent();
        try
        {
            using (engine.EnterRuntimeReadScope())
            {
                _page.Open(engine, database, table);
            }

            if (engine.TransactionManager.HasActiveTransaction(session) || MvccExecutionScope.CurrentSnapshot is not null)
            {
                // Name the columns so values arrive in table order, the order the page encodes them in.
                IEnumerable<string> names = engine.Catalog.GetTableColumns(table, database).Select(static column => column.Name);
                string sql = $"SELECT {string.Join(", ", names)} FROM {table}";
                _transactional = QueryStream.Open(engine, sql, [], session).GetEnumerator();
            }
        }
        catch
        {
            RowRefPage.Return(_page);
            _page = null;
            throw;
        }
    }

    /// <summary>Gets the row at the current position.</summary>
    public readonly RowRef Current => _page!.RowAt(_index);

    /// <summary>Advances to the next row, reading the next page when this one is used up.</summary>
    /// <returns><see langword="false"/> when the table has no more rows.</returns>
    public bool MoveNext()
    {
        RowRefPage? page = _page;
        if (page is null)
        {
            return false;
        }

        if (++_index < page.Count)
        {
            return true;
        }

        while (!page.Cursor.Done)
        {
            ReadPage(page);
            _index = 0;
            if (page.Count > 0)
            {
                return true;
            }
        }

        _index = page.Count;
        return false;
    }

    /// <summary>Ends the pass and returns its memory for reuse.</summary>
    public void Dispose()
    {
        _transactional?.Dispose();
        _transactional = null;
        if (_page is { } page)
        {
            _page = null;
            RowRefPage.Return(page);
        }
    }

    private readonly void ReadPage(RowRefPage page)
    {
        page.Clear();
        if (_transactional is { } rows)
        {
            while (page.Count < RowRefPage.Rows && rows.MoveNext())
            {
                page.Add(rows.Current);
            }

            page.Cursor.Done = page.Count < RowRefPage.Rows;
            return;
        }

        using SnapshotLockScope readScope = _engine.EnterRuntimeReadScope();
        if (_engine.Catalog.GetTableSchemaVersion(_table, _database) != page.SchemaVersion)
        {
            throw new InvalidOperationException($"Table {_table} changed shape while it was being streamed.");
        }

        if (!_engine.StorageContext.ScanRowPage(_table, _database, page.Cursor, null, null, RowRefPage.Rows, ref page, CopyRow))
        {
            throw new InvalidOperationException(
                $"Table {_table} cannot be streamed by reference here: storage has uncommitted writes for this flow.");
        }
    }
}
