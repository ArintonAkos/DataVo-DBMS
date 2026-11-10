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
public sealed class DataVoTable<T> : IEnumerable<T>
{
    /// <summary>Adapts a predicate without state to the stateful shape. Used by generated code.</summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    public static readonly Func<T, Func<T, bool>, bool> InvokePredicate = static (row, predicate) => predicate(row);

    private readonly DataVoTableInfo<T> _info;
    private readonly object _gate = new();
    private DataVoPreparedStatement?[] _statements = [];
    private string? _scanSql;

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
