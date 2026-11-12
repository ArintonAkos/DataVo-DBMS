using System.Collections;
using DataVo.Core.Parser;

namespace DataVo.Core;

/// <summary>
/// A typed, queryable view of one <c>[DataVoTable]</c> model's table. Get one from
/// <see cref="DataVoContext.Table{T}"/>.
/// </summary>
/// <remarks>
/// <para>
/// When interceptors are enabled, <c>DataVo.Generators</c> rewrites each call whose lambda it can translate into a
/// prepared statement at compile time: a primary-key or indexed equality becomes a point lookup, and comparisons
/// become an index or filtered read. Nothing is reflected over or compiled at run time.
/// </para>
/// <para>
/// A call that is not rewritten still returns the right rows; it scans the table and filters it with the
/// delegate. Scans are read a page at a time, so memory stays flat however large the table is. Pass values as <c>state</c> with a <see langword="static"/> lambda so the call can be rewritten and
/// allocates no closure.
/// </para>
/// </remarks>
/// <typeparam name="T">The model type.</typeparam>
/// <example>
/// <code>
/// Player? one = db.Table&lt;Player&gt;().FirstOrDefault(id, static (p, key) => p.Id == key);
/// foreach (Player p in db.Table&lt;Player&gt;().Where(minLevel, static (p, min) => p.Level >= min))
/// {
///     Console.WriteLine(p.Name);
/// }
/// </code>
/// </example>
public sealed partial class DataVoTable<T> : IEnumerable<T>
{
    /// <summary>Adapts a predicate without state to the stateful shape. Used by generated code.</summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    public static readonly Func<T, Func<T, bool>, bool> InvokePredicate = static (row, predicate) => predicate(row);

    private readonly DataVoTableInfo<T> _info;
    private readonly object _gate = new();
    private DataVoPreparedStatement?[] _statements = [];
    private string? _scanSql;
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string[], FusedColumns> _fusedColumns = new();

    internal DataVoTable(DataVoContext context, DataVoTableInfo<T> info)
    {
        Context = context;
        Session = context.SessionId;
        _info = info;
    }

    /// <summary>Gets the context this table reads from.</summary>
    public DataVoContext Context { get; }

    internal Guid Session { get; }

    /// <summary>Returns the first row matching a predicate, or the default value when none does.</summary>
    /// <param name="predicate">The condition a row must meet.</param>
    /// <returns>A matching row, or the default value.</returns>
    /// <example>
    /// <code>
    /// Player? admin = db.Table&lt;Player&gt;().FirstOrDefault(p => p.Name == "admin");
    /// </code>
    /// </example>
    public T? FirstOrDefault(Func<T, bool> predicate) =>
        FirstOrDefault(predicate ?? throw new ArgumentNullException(nameof(predicate)), InvokePredicate);

    /// <summary>Returns the first row matching a predicate over the row and a state value, or the default value when none does.</summary>
    /// <typeparam name="TState">The type of the value passed to the predicate.</typeparam>
    /// <param name="state">The value the predicate compares rows against.</param>
    /// <param name="predicate">The condition a row must meet.</param>
    /// <returns>A matching row, or the default value.</returns>
    /// <example>
    /// <code>
    /// Player? player = db.Table&lt;Player&gt;().FirstOrDefault(id, static (p, key) => p.Id == key);
    /// </code>
    /// </example>
    public T? FirstOrDefault<TState>(TState state, Func<T, TState, bool> predicate)
    {
        if (predicate is null)
        {
            throw new ArgumentNullException(nameof(predicate));
        }

        foreach (DataVoRow row in Scan())
        {
            T item = Read(row);
            if (predicate(item, state))
            {
                return item;
            }
        }

        return default;
    }

    /// <summary>Returns the only row matching a predicate, or the default value when none does.</summary>
    /// <param name="predicate">The condition a row must meet.</param>
    /// <returns>The matching row, or the default value.</returns>
    /// <exception cref="InvalidOperationException">More than one row matches.</exception>
    public T? SingleOrDefault(Func<T, bool> predicate) =>
        SingleOrDefault(predicate ?? throw new ArgumentNullException(nameof(predicate)), InvokePredicate);

    /// <summary>Returns the only row matching a predicate over the row and a state value, or the default value when none does.</summary>
    /// <typeparam name="TState">The type of the value passed to the predicate.</typeparam>
    /// <param name="state">The value the predicate compares rows against.</param>
    /// <param name="predicate">The condition a row must meet.</param>
    /// <returns>The matching row, or the default value.</returns>
    /// <exception cref="InvalidOperationException">More than one row matches.</exception>
    /// <example>
    /// <code>
    /// Player? player = db.Table&lt;Player&gt;().SingleOrDefault(name, static (p, n) => p.Name == n);
    /// </code>
    /// </example>
    public T? SingleOrDefault<TState>(TState state, Func<T, TState, bool> predicate)
    {
        if (predicate is null)
        {
            throw new ArgumentNullException(nameof(predicate));
        }

        T? found = default;
        bool any = false;
        foreach (DataVoRow row in Scan())
        {
            T item = Read(row);
            if (!predicate(item, state))
            {
                continue;
            }

            if (any)
            {
                throw MoreThanOne();
            }

            found = item;
            any = true;
        }

        return found;
    }

    /// <summary>Selects the rows matching a predicate. The query runs each time the result is enumerated.</summary>
    /// <param name="predicate">The condition a row must meet.</param>
    /// <returns>The matching rows.</returns>
    /// <example>
    /// <code>
    /// List&lt;Player&gt; veterans = db.Table&lt;Player&gt;().Where(p => p.Level >= 50).ToList();
    /// </code>
    /// </example>
    public DataVoQuery<T, Func<T, bool>> Where(Func<T, bool> predicate) =>
        new(this, predicate ?? throw new ArgumentNullException(nameof(predicate)), InvokePredicate, null);

    /// <summary>Selects the rows matching a predicate over the row and a state value. The query runs each time the result is enumerated.</summary>
    /// <typeparam name="TState">The type of the value passed to the predicate.</typeparam>
    /// <param name="state">The value the predicate compares rows against.</param>
    /// <param name="predicate">The condition a row must meet.</param>
    /// <returns>The matching rows.</returns>
    /// <example>
    /// <code>
    /// foreach (Player p in db.Table&lt;Player&gt;().Where((min, max), static (p, range) => p.Level >= range.min &amp;&amp; p.Level &lt;= range.max))
    /// {
    ///     Console.WriteLine(p.Name);
    /// }
    /// </code>
    /// </example>
    public DataVoQuery<T, TState> Where<TState>(TState state, Func<T, TState, bool> predicate) =>
        new(this, state, predicate ?? throw new ArgumentNullException(nameof(predicate)), null);

    /// <summary>Reads every row of the table into a list.</summary>
    /// <returns>All rows.</returns>
    public List<T> ToList()
    {
        var items = new List<T>();
        foreach (DataVoRow row in Scan())
        {
            items.Add(Read(row));
        }

        return items;
    }

    /// <summary>Enumerates every row of the table, reading a page at a time.</summary>
    /// <returns>An enumerator over all rows.</returns>
    public IEnumerator<T> GetEnumerator()
    {
        foreach (DataVoRow row in Scan())
        {
            yield return Read(row);
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <summary>
    /// Streams the table's rows as <see cref="RowRef"/> values read straight from storage, without building
    /// models. Once a thread has streamed this table, a full pass allocates nothing on the managed heap.
    /// </summary>
    /// <returns>A stream that reads the table each time it is enumerated.</returns>
    /// <remarks>
    /// Column positions follow the model's property order; <see cref="RowRefStream.GetOrdinal"/> looks one up by
    /// name. A row is valid only inside the loop iteration that produced it. See <see cref="RowRefStream"/> for
    /// how the pass interacts with writers and transactions.
    /// </remarks>
    /// <example>
    /// <code>
    /// foreach (RowRef row in db.Table&lt;Player&gt;().StreamRef())
    /// {
    ///     int id = row.GetInt32(0);
    ///     float health = row.GetFloat(1);
    /// }
    /// </code>
    /// </example>
    public RowRefStream StreamRef() => new(Context.Engine, Context.SessionId, _info.TableName);

    /// <summary>
    /// Counts the rows a compiled condition accepts. Used by generated code.
    /// </summary>
    /// <remarks>
    /// Each row is tested inside the storage scan, where it is stored, so no row is copied out. Inside a
    /// transaction the rows are streamed instead, so the count includes the transaction's own writes.
    /// </remarks>
    /// <typeparam name="TFilter">The compiled condition.</typeparam>
    /// <param name="columns">The columns the condition reads. Pass the same array on every call; it is the cache key.</param>
    /// <param name="kinds">One letter per column, saying how the condition reads it: <c>i l f d b g</c> for <c>INT</c>, <c>BIGINT</c>, <c>FLOAT</c>, <c>DOUBLE</c>, <c>BIT</c> and <c>GUID</c>, or <c>n</c> for a null test only.</param>
    /// <param name="filter">The condition, with its values already set.</param>
    /// <param name="limit">Stop after this many matches, or 0 to count them all.</param>
    /// <returns>The number of matching rows, up to <paramref name="limit"/>.</returns>
    /// <exception cref="InvalidOperationException">The table lacks one of <paramref name="columns"/>.</exception>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    public int CountMatching<TFilter>(string[] columns, string kinds, ref TFilter filter, int limit)
        where TFilter : struct, IRowRefFilter
    {
        RowRefStream rows = StreamRef();
        int[] ordinals = FusedOrdinals(columns, rows);
        filter.Bind(ordinals);
        if (rows.TryScanInPlace(ordinals, kinds, ref filter, limit, -1, RowFold.Sum, '\0', out RowAggregate inPlace))
        {
            return inPlace.Count;
        }

        int count = 0;
        foreach (RowRef row in rows)
        {
            if (filter.Matches(in row) && ++count == limit)
            {
                break;
            }
        }

        return count;
    }

    /// <summary>
    /// Folds one column over the rows a compiled condition accepts (its sum, its smallest or its largest value),
    /// without building them. Used by generated code.
    /// </summary>
    /// <remarks>
    /// Each row is tested, and its value folded in, inside the storage scan, where the row is stored. Inside a
    /// transaction the rows are streamed instead, so the result includes the transaction's own writes. A NULL cell
    /// changes nothing.
    /// </remarks>
    /// <typeparam name="TFilter">The compiled condition.</typeparam>
    /// <param name="columns">The columns the condition reads. Pass the same array on every call; it is the cache key.</param>
    /// <param name="kinds">One letter per column, saying how the condition reads it: <c>i l f d b g</c> for <c>INT</c>, <c>BIGINT</c>, <c>FLOAT</c>, <c>DOUBLE</c>, <c>BIT</c> and <c>GUID</c>, or <c>n</c> for a null test only.</param>
    /// <param name="filter">The condition, with its values already set.</param>
    /// <param name="column">The column to fold. Pass the same array on every call; it is the cache key.</param>
    /// <param name="fold">What to do with the column's values.</param>
    /// <param name="kind">The type the caller reads the column as: <c>i l f d</c> as above, or <c>t</c> for <c>DATE</c>.</param>
    /// <param name="totals">The result.</param>
    /// <returns><see langword="false"/> when the column is not stored as that type; the caller then reads rows.</returns>
    /// <exception cref="InvalidOperationException">The table lacks one of the columns.</exception>
    /// <exception cref="OverflowException">An integer sum left the range of <see cref="long"/>.</exception>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    public bool FoldMatching<TFilter>(string[] columns, string kinds, ref TFilter filter, string[] column, RowFold fold, char kind, out RowFoldTotals totals)
        where TFilter : struct, IRowRefFilter
    {
        totals = default;
        RowRefStream rows = StreamRef();
        int[] ordinals = FusedOrdinals(columns, rows);
        int foldOrdinal = FusedOrdinals(column, rows)[0];
        filter.Bind(ordinals);
        if (!rows.TryScanInPlace(ordinals, kinds, ref filter, 0, foldOrdinal, fold, kind, out RowAggregate found))
        {
            found = default;
            RowFoldKind reader = RowFoldKind.None;
            foreach (RowRef row in rows)
            {
                if (reader == RowFoldKind.None && (reader = RowAggregate.KindFor(row.KindOfColumn(foldOrdinal), fold, kind)) == RowFoldKind.None)
                {
                    return false;
                }

                if (filter.Matches(in row))
                {
                    found.Add(in row, reader, foldOrdinal);
                }
            }
        }

        totals.Values = found.Values;
        totals.Whole = found.Whole;
        totals.Real = found.Real;
        return true;
    }

    /// <summary>
    /// Counts the rows a compiled condition accepts, from the table's columnar index when it has one that
    /// holds every column the condition reads, and as <see cref="CountMatching"/> does otherwise. Used by
    /// generated code.
    /// </summary>
    /// <typeparam name="TFilter">The compiled condition.</typeparam>
    /// <param name="columns">The columns the condition reads. Pass the same array on every call; it is the cache key.</param>
    /// <param name="kinds">One letter per column; see <see cref="CountMatching"/>.</param>
    /// <param name="filter">The condition, with its values already set.</param>
    /// <param name="limit">Stop after this many matches, or 0 to count them all.</param>
    /// <returns>The number of matching rows, up to <paramref name="limit"/>.</returns>
    /// <exception cref="InvalidOperationException">The table lacks one of <paramref name="columns"/>.</exception>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    public int CountMatchingColumnar<TFilter>(string[] columns, string kinds, ref TFilter filter, int limit)
        where TFilter : struct, IRowRefFilter, IColumnarFilter =>
        TryColumnar(columns, ref filter, null, RowFold.Sum, '\0', limit, out RowAggregate found)
            ? found.Count
            : CountMatching(columns, kinds, ref filter, limit);

    /// <summary>
    /// Folds one column over the rows a compiled condition accepts, from the table's columnar index when it
    /// has one that holds every column involved, and as <see cref="FoldMatching"/> does otherwise. Used by
    /// generated code.
    /// </summary>
    /// <typeparam name="TFilter">The compiled condition.</typeparam>
    /// <param name="columns">The columns the condition reads. Pass the same array on every call; it is the cache key.</param>
    /// <param name="kinds">One letter per column; see <see cref="CountMatching"/>.</param>
    /// <param name="filter">The condition, with its values already set.</param>
    /// <param name="column">The column to fold. Pass the same array on every call; it is the cache key.</param>
    /// <param name="fold">What to do with the column's values.</param>
    /// <param name="kind">The type the caller reads the column as; see <see cref="FoldMatching"/>.</param>
    /// <param name="totals">The result.</param>
    /// <returns><see langword="false"/> when the column is not stored as that type; the caller then reads rows.</returns>
    /// <exception cref="InvalidOperationException">The table lacks one of the columns.</exception>
    /// <exception cref="OverflowException">An integer sum left the range of <see cref="long"/>.</exception>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    public bool FoldMatchingColumnar<TFilter>(string[] columns, string kinds, ref TFilter filter, string[] column, RowFold fold, char kind, out RowFoldTotals totals)
        where TFilter : struct, IRowRefFilter, IColumnarFilter
    {
        if (!TryColumnar(columns, ref filter, column, fold, kind, 0, out RowAggregate found))
        {
            return FoldMatching(columns, kinds, ref filter, column, fold, kind, out totals);
        }

        totals = default;
        totals.Values = found.Values;
        totals.Whole = found.Whole;
        totals.Real = found.Real;
        return true;
    }

    /// <summary>
    /// Answers a count or a fold from the table's columnar index. Returns false, leaving the row path to answer,
    /// when the table has no such index or it lacks a column, inside a transaction (the index holds committed
    /// rows only), or when the condition or the fold has no columnar form.
    /// </summary>
    private bool TryColumnar<TFilter>(string[] columns, ref TFilter filter, string[]? column, RowFold fold, char kind, int limit, out RowAggregate found)
        where TFilter : struct, IColumnarFilter
    {
        found = default;
        Runtime.DataVoEngine engine = Context.Engine;
        if (engine.StorageContext.ColumnarStore is null
            || engine.TransactionManager.HasActiveTransaction(Context.SessionId)
            || MVCC.MvccExecutionScope.CurrentSnapshot is not null
            || engine.Sessions.Get(Context.SessionId) is not { Length: > 0 } database)
        {
            return false;
        }

        using Runtime.SnapshotLockScope readScope = engine.EnterRuntimeReadScope();
        Indexing.Columnar.ColumnarIndex? index = engine.Columnar.Find(database, _info.TableName);
        if (index is null)
        {
            return false;
        }

        RowRefStream rows = StreamRef();
        Indexing.Columnar.ColumnarScan.Workspace workspace = Indexing.Columnar.ColumnarScan.Rent();
        var builder = new ColumnarPredicateBuilder(workspace.Steps, FusedOrdinals(columns, rows));
        filter.Describe(ref builder);
        int steps = builder.Steps(out bool complete).Length;
        if (!complete || !Indexing.Columnar.ColumnarScan.TryBind(index, workspace.Steps.AsSpan(0, steps)))
        {
            return false;
        }

        int folds = 0;
        if (column is not null)
        {
            int slot = index.SlotOf(FusedOrdinals(column, rows)[0]);
            RowFoldKind folded = slot < 0 ? RowFoldKind.None : RowAggregate.KindFor(index.KindOf(slot), fold, kind);
            if (folded == RowFoldKind.None)
            {
                return false;
            }

            workspace.Folds[0] = new Indexing.Columnar.ColumnarFold { Slot = slot, Kind = folded };
            folds = 1;
        }

        if (!Indexing.Columnar.ColumnarScan.TryRun(
                index,
                new ReadOnlySpan<ColumnarStep>(workspace.Steps, 0, steps),
                new ReadOnlySpan<Indexing.Columnar.ColumnarFold>(workspace.Folds, 0, folds),
                workspace.Totals,
                limit,
                out int matched))
        {
            return false;
        }

        found = folds == 0 ? default : workspace.Totals[0];
        found.Count = matched;
        return true;
    }

    /// <summary>Finds where a compiled condition's columns sit in the table, once per schema version.</summary>
    private int[] FusedOrdinals(string[] columns, RowRefStream rows)
    {
        int version = rows.SchemaVersion;
        if (_fusedColumns.TryGetValue(columns, out FusedColumns? known) && known.SchemaVersion == version)
        {
            return known.Ordinals;
        }

        int[] ordinals = new int[columns.Length];
        for (int i = 0; i < ordinals.Length; i++)
        {
            try
            {
                ordinals[i] = rows.GetOrdinal(columns[i]);
            }
            catch (ArgumentException exception)
            {
                throw new InvalidOperationException(exception.Message, exception);
            }
        }

        _fusedColumns[columns] = new FusedColumns(version, ordinals);
        return ordinals;
    }

    private sealed class FusedColumns(int schemaVersion, int[] ordinals)
    {
        public int SchemaVersion { get; } = schemaVersion;

        public int[] Ordinals { get; } = ordinals;
    }

    internal static InvalidOperationException MoreThanOne() => new("Sequence contains more than one matching element");

    internal T Read(Dictionary<string, object?> row) => _info.ReadRow(row);

    internal T Read(DataVoRow row) => _info.ReadStreamedRow(row);

    /// <summary>Streams every row of the table.</summary>
    internal IEnumerable<DataVoRow> Scan() => Stream(null, []);

    /// <summary>Streams the rows meeting a condition, or all rows when there is none.</summary>
    internal IEnumerable<DataVoRow> Stream(string? where, DataVoQueryParameter[] parameters)
    {
        string sql = where is null
            ? _scanSql ??= $"SELECT {_info.ColumnList} FROM {_info.TableName}"
            : $"SELECT {_info.ColumnList} FROM {_info.TableName} WHERE {where}";
        return QueryStream.Open(Context.Engine, sql, parameters, Context.SessionId);
    }

    /// <summary>Gets the statement held in a slot, preparing it on first use.</summary>
    internal DataVoPreparedStatement Statement(int slot, string where, int limit)
    {
        DataVoPreparedStatement?[] statements = Volatile.Read(ref _statements);
        if ((uint)slot < (uint)statements.Length && Volatile.Read(ref statements[slot]) is { } ready)
        {
            return ready;
        }

        lock (_gate)
        {
            statements = _statements;
            if (slot >= statements.Length)
            {
                Array.Resize(ref statements, Math.Max(slot + 1, statements.Length * 2));
                Volatile.Write(ref _statements, statements);
            }

            if (statements[slot] is { } raced)
            {
                return raced;
            }

            string sql = $"SELECT {_info.ColumnList} FROM {_info.TableName} WHERE {where}";
            DataVoPreparedStatement prepared = Context.Prepare(limit > 0 ? $"{sql} LIMIT {limit}" : sql);
            Volatile.Write(ref statements[slot], prepared);
            return prepared;
        }
    }
}

/// <summary>Hands out the statement slots of one model's compiled queries.</summary>
internal static class DataVoLinqPlanSlots<T>
{
    private static int s_next = -1;

    public static int Next() => Interlocked.Increment(ref s_next);
}
