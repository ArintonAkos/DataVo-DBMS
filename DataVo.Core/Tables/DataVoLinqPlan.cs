using System.ComponentModel;
using DataVo.Core.Annotations;
using DataVo.Core.Contracts.Results;

namespace DataVo.Core;

/// <summary>
/// One query that <c>DataVo.Generators</c> translated at compile time. Generated interceptors derive a plan per
/// call site and route the call to it; application code does not use this type directly.
/// </summary>
/// <typeparam name="T">The model type.</typeparam>
/// <typeparam name="TState">The type of the value the query's predicate receives.</typeparam>
[EditorBrowsable(EditorBrowsableState.Never)]
public abstract class DataVoLinqPlan<T, TState>
{
    private const int All = 0;
    private const int First = 1;
    private const int Single = 2;

    private readonly int _slot = DataVoLinqPlanSlots<T>.Next() * 3;
    private readonly string _where;
    private readonly bool _unique;

    /// <summary>Creates a plan.</summary>
    /// <param name="where">The predicate as a SQL condition over <c>@p0</c>, <c>@p1</c>, and so on.</param>
    /// <param name="unique">Whether the condition pins the primary key, so at most one row can match.</param>
    protected DataVoLinqPlan(string where, bool unique)
    {
        _where = where ?? throw new ArgumentNullException(nameof(where));
        _unique = unique;
    }

    /// <summary>Gets whether a state value can be bound. A plan returns false for a null it cannot compare in SQL.</summary>
    /// <param name="state">The value passed to the query.</param>
    /// <returns><see langword="true"/> when the statement answers the query for this state.</returns>
    protected virtual bool CanBind(TState state) => true;

    /// <summary>Binds the state to the statement's parameters and executes it.</summary>
    /// <param name="statement">The prepared statement for this plan.</param>
    /// <param name="state">The value passed to the query.</param>
    /// <param name="borrowed">Whether the result is read immediately and may be a reused instance.</param>
    /// <returns>The statement's result.</returns>
    protected abstract QueryResult Run(DataVoPreparedStatement statement, TState state, bool borrowed);

    /// <summary>Binds the state to a new parameter array, for a streamed scan.</summary>
    /// <param name="state">The value passed to the query.</param>
    /// <returns>The statement's parameters, in order.</returns>
    protected abstract DataVoQueryParameter[] Parameters(TState state);

    /// <summary>
    /// Counts the matching rows with a condition compiled into the scan, when the generator produced one.
    /// </summary>
    /// <param name="table">The table to read.</param>
    /// <param name="state">The value passed to the query.</param>
    /// <param name="limit">Stop after this many matches, or 0 to count them all.</param>
    /// <param name="count">The number of matching rows, up to <paramref name="limit"/>.</param>
    /// <returns><see langword="false"/> when this plan has no compiled condition, so the caller reads rows instead.</returns>
    protected virtual bool TryCount(DataVoTable<T> table, TState state, int limit, out int count)
    {
        count = 0;
        return false;
    }

    /// <summary>
    /// Sums one column over the matching rows with a condition compiled into the scan, when the generator
    /// produced one.
    /// </summary>
    /// <param name="table">The table to read.</param>
    /// <param name="state">The value passed to the query.</param>
    /// <param name="column">The column to sum, as a one-element array that is the same instance on every call.</param>
    /// <param name="floating">Whether the sum is over a <c>FLOAT</c> or <c>DOUBLE</c> column.</param>
    /// <param name="whole">The sum, when <paramref name="floating"/> is false.</param>
    /// <param name="real">The sum, when <paramref name="floating"/> is true.</param>
    /// <returns><see langword="false"/> when this plan cannot sum in the scan, so the caller reads rows instead.</returns>
    protected virtual bool TrySum(DataVoTable<T> table, TState state, string[] column, bool floating, out long whole, out double real)
    {
        whole = 0;
        real = 0;
        return false;
    }

    /// <summary>Sums a column over the matching rows without building them, when the plan has a compiled condition.</summary>
    internal bool TrySumMatching(DataVoTable<T> table, TState state, string[] column, bool floating, out long whole, out double real)
    {
        if (_unique)
        {
            whole = 0;
            real = 0;
            return false;
        }

        return TrySum(table, state, column, floating, out whole, out real);
    }

    /// <summary>Returns the first matching row, or the default value when none matches.</summary>
    /// <param name="table">The table to read.</param>
    /// <param name="state">The value passed to the query.</param>
    /// <param name="predicate">The original predicate, used when the state cannot be bound.</param>
    /// <returns>A matching row, or the default value.</returns>
    public T? FirstOrDefault(DataVoTable<T> table, TState state, Func<T, TState, bool> predicate)
    {
        if (!CanBind(state))
        {
            return table.FirstOrDefault(state, predicate);
        }

        QueryResult result = Execute(table, state, _unique ? All : First, borrowed: true);
        return result.Data.Count == 0 ? default : table.Read(result.Data[0]);
    }

    /// <summary>Returns the only matching row, or the default value when none matches.</summary>
    /// <param name="table">The table to read.</param>
    /// <param name="state">The value passed to the query.</param>
    /// <param name="predicate">The original predicate, used when the state cannot be bound.</param>
    /// <returns>The matching row, or the default value.</returns>
    /// <exception cref="InvalidOperationException">More than one row matches.</exception>
    public T? SingleOrDefault(DataVoTable<T> table, TState state, Func<T, TState, bool> predicate)
    {
        if (!CanBind(state))
        {
            return table.SingleOrDefault(state, predicate);
        }

        QueryResult result = Execute(table, state, _unique ? All : Single, borrowed: true);
        return result.Data.Count switch
        {
            0 => default,
            1 => table.Read(result.Data[0]),
            _ => throw DataVoTable<T>.MoreThanOne(),
        };
    }

    /// <summary>Creates the deferred query for a <c>Where</c> call.</summary>
    /// <param name="table">The table to read.</param>
    /// <param name="state">The value passed to the query.</param>
    /// <param name="predicate">The original predicate, used when the state cannot be bound.</param>
    /// <returns>A query that runs this plan when enumerated.</returns>
    public DataVoQuery<T, TState> Where(DataVoTable<T> table, TState state, Func<T, TState, bool> predicate) =>
        new(table, state, predicate, this);

    /// <summary>Whether at most one row can match, so a point lookup answers an enumeration.</summary>
    internal bool IsUnique => _unique;

    internal bool Binds(TState state) => CanBind(state);

    /// <summary>Reads the one row a unique plan can match.</summary>
    internal bool TryFirst(DataVoTable<T> table, TState state, bool borrowed, out T item)
    {
        QueryResult result = Execute(table, state, _unique ? All : First, borrowed);
        if (result.Data.Count == 0)
        {
            item = default!;
            return false;
        }

        item = table.Read(result.Data[0]);
        return true;
    }

    /// <summary>Gets whether any row matches, reading at most one.</summary>
    internal bool Any(DataVoTable<T> table, TState state) =>
        !_unique && TryCount(table, state, 1, out int found)
            ? found > 0
            : Execute(table, state, _unique ? All : First, borrowed: true).Data.Count > 0;

    /// <summary>Counts the matching rows without building them, when the plan has a compiled condition.</summary>
    internal bool TryCountMatching(DataVoTable<T> table, TState state, out int count)
    {
        if (_unique)
        {
            count = 0;
            return false;
        }

        return TryCount(table, state, 0, out count);
    }

    /// <summary>Opens a paged scan of the matching rows.</summary>
    internal IEnumerable<DataVoRow> Stream(DataVoTable<T> table, TState state) => table.Stream(_where, Parameters(state));

    private QueryResult Execute(DataVoTable<T> table, TState state, int kind, bool borrowed)
    {
        DataVoPreparedStatement statement = table.Statement(_slot + kind, _where, kind);
        QueryResult result = Run(statement, state, borrowed);
        if (result.IsError)
        {
            DataVoGeneratedTableSupport.ThrowIfError(result, statement.Sql);
        }

        return result;
    }
}
