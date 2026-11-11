using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Reactive;
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
    /// Counts the rows a compiled condition accepts, and optionally sums one of their columns, by testing each
    /// row inside the storage scan, where it is stored: no row is copied out, and only the cells up to the last
    /// column the scan reads are located.
    /// </summary>
    /// <param name="ordinals">The table positions of the columns the condition reads.</param>
    /// <param name="kinds">One letter per column, saying how the condition reads it; see <see cref="RowRefPage.PlanDirect"/>.</param>
    /// <param name="filter">The condition, already bound to <paramref name="ordinals"/>.</param>
    /// <param name="limit">Stop after this many matches, or 0 to visit them all.</param>
    /// <param name="sumOrdinal">The table position of the column to sum; ignored when nothing is summed.</param>
    /// <param name="floating">Whether the sum is a floating-point one.</param>
    /// <param name="summed">Whether a column is summed at all.</param>
    /// <param name="result">The totals over the matching rows.</param>
    /// <returns>
    /// <see langword="false"/> when the rows cannot be read in place (inside a transaction, or while storage holds
    /// uncommitted writes for this flow) or the summed column's type does not fit the sum; the caller then takes
    /// another path. <paramref name="result"/> is meaningless in that case.
    /// </returns>
    /// <remarks>
    /// The scan is generic over the condition from here into the storage engine's own row loop, so the condition
    /// and the totals are compiled into that loop: no delegate is called per row. The read lock is held for one
    /// block of rows at a time, as it is for a streamed pass, so writers run between blocks and the same
    /// consistency holds: a row that existed for the whole scan is visited exactly once.
    /// <para>
    /// Where the table's layout allows, a row is tested without building a <see cref="RowRef"/>: stored bytes are
    /// read at offsets fixed for the whole scan, and in-memory rows from their cells. Each row is first checked
    /// against that layout (its leading null flags, or its cells' types); a row that differs, such as one with a
    /// NULL among the leading columns or one written before a column was added, is tested through a
    /// <see cref="RowRef"/> instead, so the result is the same either way.
    /// </para>
    /// </remarks>
    internal bool TryScanInPlace<TFilter>(int[] ordinals, string kinds, ref TFilter filter, int limit, int sumOrdinal, bool floating, bool summed, out RowAggregate result)
        where TFilter : struct, IRowRefFilter
    {
        result = default;
        DataVoEngine engine = _engine ?? throw Uninitialized();
        string database = DatabaseOf(engine, _session);
        if (engine.TransactionManager.HasActiveTransaction(_session) || MvccExecutionScope.CurrentSnapshot is not null)
        {
            return false;
        }

        int columns = summed ? sumOrdinal + 1 : 0;
        ulong needed = summed && sumOrdinal < 64 ? 1UL << sumOrdinal : 0;
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

            RowSumKind kind = RowSumKind.None;
            if (summed && (kind = RowAggregate.KindFor(page.KindAt(sumOrdinal), floating)) == RowSumKind.None)
            {
                return false;
            }

            RowRefPage.DirectLayout layout = page.PlanDirect(ordinals, kinds, columns, summed ? sumOrdinal : -1);
            if (layout.Bytes)
            {
                filter.BindOffsets(new ReadOnlySpan<int>(layout.ValueOffsets, 0, ordinals.Length));
            }

            var scan = new InPlaceScan<TFilter>(filter, page, columns, needed, limit, kind, sumOrdinal, layout);

            // A limited scan (Any) starts with a small block so a match near the start costs little.
            int block = limit > 0 ? 64 : InPlaceBlockRows;
            while (!page.Cursor.Done && !scan.Full)
            {
                using SnapshotLockScope readScope = engine.EnterRuntimeReadScope();
                if (engine.Catalog.GetTableSchemaVersion(_table!, database) != page.SchemaVersion)
                {
                    throw new InvalidOperationException($"Table {_table} changed shape while it was being scanned.");
                }

                if (!engine.StorageContext.ScanRowBlock(_table!, database, page.Cursor, block, ref scan))
                {
                    return false;
                }

                block = InPlaceBlockRows;
            }

            result = scan.Totals;
            return true;
        }
        finally
        {
            RowRefPage.Return(page);
        }
    }

    // Rows examined per hold of the read lock. Larger than a streamed page: nothing is copied while it is held.
    private const int InPlaceBlockRows = 4096;

    /// <summary>
    /// The visitor of an in-place scan: the condition, the totals, and the page whose layout locates each row's
    /// cells. A struct, so the storage engine's row loop is compiled around it.
    /// </summary>
    private struct InPlaceScan<TFilter> : StorageContext.IStoredRowVisitor
        where TFilter : struct, IRowRefFilter
    {
        private TFilter _filter;
        private readonly RowRefPage _page;
        private readonly int _columns;
        private readonly ulong _needed;
        private readonly int _limit;
        private readonly RowSumKind _sum;
        private readonly int _sumOrdinal;

        public RowAggregate Totals;

        private readonly RowRefPage.DirectLayout _direct;

        public InPlaceScan(TFilter filter, RowRefPage page, int columns, ulong needed, int limit, RowSumKind sum, int sumOrdinal, RowRefPage.DirectLayout direct)
        {
            _direct = direct;
            _filter = filter;
            _page = page;
            _columns = columns;
            _needed = needed;
            _limit = limit;
            _sum = sum;
            _sumOrdinal = sumOrdinal;
            Totals = default;
        }

        public readonly bool Full => _limit > 0 && Totals.Count >= _limit;

        public void Visit(RowKey rowId, ReadOnlySpan<byte> stored, StoredRow? typed)
        {
            if (Full)
            {
                return;
            }

            if (typed is null)
            {
                if (_direct.Bytes && LaidOutAsExpected(stored))
                {
                    if (_filter.MatchesBytes(stored))
                    {
                        Totals.AddBytes(stored, _sum, _direct.SumOffset);
                    }

                    return;
                }
            }
            else if (_direct.Cells)
            {
                ReadOnlySpan<CellValue> cells = typed.Cells;
                if (HoldsExpectedCells(cells))
                {
                    if (_filter.MatchesCells(cells))
                    {
                        Totals.AddCells(cells, _sum, _sumOrdinal);
                    }

                    return;
                }
            }

            RowRef row = typed is null
                ? _page.InPlace(rowId, stored, _columns)
                : _page.InPlace(rowId, typed, _columns, _needed);
            if (_filter.Matches(in row))
            {
                Totals.Add(in row, _sum, _sumOrdinal);
            }
        }

        /// <summary>
        /// Whether a stored row's leading cells are all present, so each value is at its fixed offset. Checking
        /// the flags in order is enough: a flag found where it is expected means the cell before it was present.
        /// </summary>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private readonly bool LaidOutAsExpected(ReadOnlySpan<byte> stored)
        {
            if (stored.Length < _direct.MinLength)
            {
                return false;
            }

            int[] offsets = _direct.FlagOffsets;
            byte[] expected = _direct.FlagExpected;
            for (int i = 0; i < _direct.FlagCount; i++)
            {
                if (stored[offsets[i]] != expected[i])
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>Whether a typed row holds, for every column read, a cell of the column's own type (so not NULL).</summary>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private readonly bool HoldsExpectedCells(ReadOnlySpan<CellValue> cells)
        {
            int[] ordinals = _direct.CellOrdinals;
            byte[] types = _direct.CellTypes;
            for (int i = 0; i < _direct.CellCount; i++)
            {
                int ordinal = ordinals[i];
                if ((uint)ordinal >= (uint)cells.Length || (byte)cells[ordinal].Type != types[i])
                {
                    return false;
                }
            }

            return true;
        }
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
