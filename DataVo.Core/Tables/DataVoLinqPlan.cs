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

    /// <summary>Reads the matching rows, or returns null when the state cannot be bound and the caller must scan.</summary>
    internal List<Dictionary<string, object?>>? Rows(DataVoTable<T> table, TState state, bool firstOnly) =>
        CanBind(state) ? Execute(table, state, firstOnly && !_unique ? First : All, borrowed: false).Data : null;

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
