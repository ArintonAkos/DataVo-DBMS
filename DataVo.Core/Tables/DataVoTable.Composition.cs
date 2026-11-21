using System.Collections.Concurrent;
using System.ComponentModel;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Parser;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.Core;

/// <summary>
/// Sorting, paging, projection and bulk changes over a whole table. Each method starts from the query of every
/// row and behaves as the same method on <see cref="DataVoQuery{T, TState}"/>.
/// </summary>
public sealed partial class DataVoTable<T>
{
    private static readonly Func<T, bool> EveryRow = static _ => true;

    private readonly ConcurrentDictionary<string, DataVoPreparedStatement> _composedStatements = new(StringComparer.Ordinal);

    /// <summary>Gets the query of every row; its plan reads the table without a condition.</summary>
    internal DataVoQuery<T, Func<T, bool>> All => new(this, EveryRow, InvokePredicate, DataVoTablePlan<T>.Instance);

    /// <summary>Sorts the table's rows by a key, smallest first. Ties keep storage order.</summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="keySelector">The key, as <c>p =&gt; p.Column</c> to sort in the engine.</param>
    /// <returns>The sorted rows; chain <c>ThenBy</c>, <c>Skip</c>, <c>Take</c> or <c>Select</c>.</returns>
    /// <example>
    /// <code>
    /// List&lt;Player&gt; ranking = db.Table&lt;Player&gt;().OrderByDescending(static p => p.Score).Take(10).ToList();
    /// </code>
    /// </example>
    public DataVoOrderedQuery<T, Func<T, bool>> OrderBy<TKey>(Func<T, TKey> keySelector) => All.OrderBy(keySelector);

    /// <summary>Sorts the table's rows by a key with a comparer, smallest first. A comparer always sorts in memory.</summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="keySelector">The key.</param>
    /// <param name="comparer">Orders two keys; <see langword="null"/> for the default order.</param>
    /// <returns>The sorted rows.</returns>
    public DataVoOrderedQuery<T, Func<T, bool>> OrderBy<TKey>(Func<T, TKey> keySelector, IComparer<TKey>? comparer) => All.OrderBy(keySelector, comparer);

    /// <summary>Sorts the table's rows by a key, largest first. Ties keep storage order.</summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="keySelector">The key, as <c>p =&gt; p.Column</c> to sort in the engine.</param>
    /// <returns>The sorted rows.</returns>
    public DataVoOrderedQuery<T, Func<T, bool>> OrderByDescending<TKey>(Func<T, TKey> keySelector) => All.OrderByDescending(keySelector);

    /// <summary>Sorts the table's rows by a key with a comparer, largest first. A comparer always sorts in memory.</summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="keySelector">The key.</param>
    /// <param name="comparer">Orders two keys; <see langword="null"/> for the default order.</param>
    /// <returns>The sorted rows.</returns>
    public DataVoOrderedQuery<T, Func<T, bool>> OrderByDescending<TKey>(Func<T, TKey> keySelector, IComparer<TKey>? comparer) => All.OrderByDescending(keySelector, comparer);

    /// <summary>Sorts the table's rows by a compiled column. Used by generated code.</summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="key">The compiled column.</param>
    /// <param name="keySelector">The original key, used when the query runs in memory.</param>
    /// <returns>The sorted rows.</returns>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public DataVoOrderedQuery<T, Func<T, bool>> OrderByColumn<TKey>(DataVoOrderKey key, Func<T, TKey> keySelector) => All.OrderByColumn(key, keySelector);

    /// <summary>Skips rows in storage order, as <see cref="Enumerable.Skip{TSource}"/> does. The engine skips them.</summary>
    /// <param name="count">The rows to skip; zero or less skips none.</param>
    /// <returns>The remaining rows.</returns>
    public DataVoPagedQuery<T, Func<T, bool>> Skip(int count) => All.Skip(count);

    /// <summary>Keeps the first rows in storage order, as <see cref="Enumerable.Take{TSource}(IEnumerable{TSource}, int)"/> does. The engine stops reading after them.</summary>
    /// <param name="count">The rows to keep; zero or less keeps none.</param>
    /// <returns>The kept rows.</returns>
    public DataVoPagedQuery<T, Func<T, bool>> Take(int count) => All.Take(count);

    /// <summary>
    /// Projects each row. When <c>DataVo.Generators</c> compiles <paramref name="selector"/>, only the columns it
    /// reads are selected and no model is built.
    /// </summary>
    /// <typeparam name="TResult">The type of the projected value.</typeparam>
    /// <param name="selector">Builds the value from a row.</param>
    /// <returns>The projected rows.</returns>
    /// <example>
    /// <code>
    /// List&lt;string&gt; names = db.Table&lt;Player&gt;().Select(static p => p.Name).ToList(); // reads only Name
    /// </code>
    /// </example>
    public DataVoProjectedQuery<T, Func<T, bool>, TResult> Select<TResult>(Func<T, TResult> selector) => All.Select(selector);

    /// <summary>Projects each row with a compiled projection. Used by generated code.</summary>
    /// <typeparam name="TResult">The type of the projected value.</typeparam>
    /// <param name="projection">The compiled projection.</param>
    /// <param name="selector">The original selector, used when the query runs in memory.</param>
    /// <returns>The projected rows.</returns>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public DataVoProjectedQuery<T, Func<T, bool>, TResult> SelectColumns<TResult>(DataVoProjection<T, TResult> projection, Func<T, TResult> selector) =>
        All.SelectColumns(projection, selector);

    /// <summary>Deletes every row of the table with one <c>DELETE</c> statement.</summary>
    /// <returns>The number of rows deleted.</returns>
    public int ExecuteDelete() => All.ExecuteDelete();

    /// <summary>Sets columns of every row with one <c>UPDATE</c> statement. The call must be compiled; see <see cref="DataVoQuery{T, TState}.ExecuteUpdate(Func{DataVoSetters{T}, DataVoSetters{T}})"/>.</summary>
    /// <param name="setters">The columns to set and their values.</param>
    /// <returns>The number of rows updated.</returns>
    /// <exception cref="NotSupportedException">The call was not compiled by <c>DataVo.Generators</c>.</exception>
    public int ExecuteUpdate(Func<DataVoSetters<T>, DataVoSetters<T>> setters) => All.ExecuteUpdate(setters);

    /// <summary>Sets columns of every row from a state value, with one <c>UPDATE</c> statement. The call must be compiled.</summary>
    /// <typeparam name="TValue">The type of the state value.</typeparam>
    /// <param name="value">The value the setters read.</param>
    /// <param name="setters">The columns to set and their values.</param>
    /// <returns>The number of rows updated.</returns>
    /// <exception cref="NotSupportedException">The call was not compiled by <c>DataVo.Generators</c>.</exception>
    public int ExecuteUpdate<TValue>(TValue value, Func<DataVoSetters<T>, TValue, DataVoSetters<T>> setters) => All.ExecuteUpdate(value, setters);

    /// <summary>Runs a compiled update over every row. Used by generated code.</summary>
    /// <param name="assignments">The SET list over <c>@u0</c>, <c>@u1</c>...</param>
    /// <param name="values">The values of the SET list.</param>
    /// <returns>The number of rows updated.</returns>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public int ExecuteUpdateSet(string assignments, DataVoQueryParameter[] values) => All.ExecuteUpdateSet(assignments, values);

    /// <summary>
    /// Registers a strongly-typed reactive subscription observing every row of this table.
    /// Deltas are delivered when <see cref="DataVoContext.DispatchPendingNotifications"/> is called.
    /// </summary>
    /// <param name="onChanged">The callback invoked with typed added, removed, and updated rows.</param>
    /// <returns>A disposable subscription handle.</returns>
    /// <example>
    /// <code>
    /// using var sub = db.Table&lt;Player&gt;().Subscribe(change => Handle(change.Added));
    /// </code>
    /// </example>
    public IDisposable Subscribe(Action<QueryChange<T>> onChanged) => All.Subscribe(onChanged);

    /// <summary>
    /// Registers a zero-allocation buffered window subscription over this table, writing up to
    /// <c>destination.Length</c> live rows into <paramref name="destination"/> on each drain.
    /// </summary>
    /// <param name="destination">Caller-owned buffer receiving the live rows.</param>
    /// <param name="onUpdated">Callback invoked with the number of rows written whenever the window changes.</param>
    /// <returns>A disposable subscription handle.</returns>
    public IDisposable SubscribeInto(Memory<T> destination, Action<int> onUpdated) => All.SubscribeInto(destination, onUpdated);

    /// <summary>Gets the table's name.</summary>
    internal string TableName => _info.TableName;

    /// <summary>Gets every column of the model, as a SELECT list.</summary>
    internal string ModelColumns => _info.ColumnList;

    /// <summary>Gets the model's column names in reader ordinal order.</summary>
    internal string[] ModelColumnNames => _info.ColumnNames.Length > 0
        ? _info.ColumnNames
        : [.. _info.ColumnList.Split(',').Select(static name => name.Trim())];

    /// <summary>Gets the zero-allocation reactive row reader generated for this model, if available.</summary>
    internal DataVoReactiveRowReader<T>? ReactiveRowReader => _info.ReadReactiveRow;

    /// <summary>Gets or prepares a reusable statement cached by its SQL text on this table.</summary>
    internal DataVoPreparedStatement GetOrPrepareStatement(string sql) =>
        _composedStatements.GetOrAdd(sql, static (text, context) => context.Prepare(text), Context);

    /// <summary>Gets the reader that builds a model from a row holding <see cref="ModelColumns"/>.</summary>
    internal Func<DataVoRow, T> RowReader => _info.ReadStreamedRow;

    /// <summary>Gets the primary-key column.</summary>
    internal string KeyColumn => _info.KeyColumn ?? throw new InvalidOperationException(
        $"The generated description of {_info.TableName} names no primary key; rebuild the project with the current DataVo.Generators.");

    /// <summary>Runs a SELECT and streams its rows; a sorted query is read whole first, as the engine sorts it.</summary>
    /// <param name="columns">The SELECT list.</param>
    /// <param name="where">The condition, or <see langword="null"/> for every row.</param>
    /// <param name="parameters">The condition's values.</param>
    /// <param name="orderBy">The ORDER BY list, or <see langword="null"/>.</param>
    /// <param name="skip">The rows to skip.</param>
    /// <param name="limit">The row limit, or -1 for none.</param>
    /// <returns>The rows.</returns>
    internal IEnumerable<DataVoRow> Query(string columns, string? where, DataVoQueryParameter[] parameters, string? orderBy, long skip, long limit)
    {
        string sql = DataVoSql.Select(columns, _info.TableName, where, orderBy, limit >= 0, skip > 0);
        return QueryStream.Open(Context.Engine, sql, DataVoSql.Paged(parameters, skip, limit), Context.SessionId);
    }

    /// <summary>Runs a DELETE or UPDATE through a statement prepared once per SQL text.</summary>
    /// <param name="sql">The statement.</param>
    /// <param name="parameters">Its values.</param>
    /// <returns>The number of rows the statement changed.</returns>
    internal int Change(string sql, DataVoQueryParameter[] parameters)
    {
        DataVoPreparedStatement statement = _composedStatements.GetOrAdd(sql, static (text, context) => context.Prepare(text), Context);
        QueryResult result = statement.Execute(parameters);
        Annotations.DataVoGeneratedTableSupport.ThrowIfError(result, sql);
        return DataVoSql.AffectedRows(result);
    }

    /// <summary>
    /// Runs a statement once per primary-key value, for a change whose condition only C# can evaluate. The keys
    /// are read first, and the statements run in one transaction (the caller's, when one is open), so the
    /// change is applied whole or not at all.
    /// </summary>
    /// <param name="keys">The primary-key values of the rows to change.</param>
    /// <param name="sql">The statement, whose condition pins <c>@key</c>.</param>
    /// <param name="values">The statement's other values.</param>
    /// <returns>The number of rows changed.</returns>
    internal int ChangeEachKey(IEnumerable<object?> keys, string sql, DataVoQueryParameter[] values)
    {
        List<object?> pending = [.. keys];
        if (pending.Count == 0)
        {
            return 0;
        }

        return InTransaction(() =>
        {
            int changed = 0;
            foreach (object? key in pending)
            {
                changed += Change(sql, DataVoSql.Concat(values, [DataVoQueryParameter.FromObject(DataVoSql.KeyParameter, key)]));
            }

            return changed;
        });
    }

    /// <summary>Runs <paramref name="work"/> inside the session's transaction, opening and committing one when none is open.</summary>
    private int InTransaction(Func<int> work)
    {
        if (Context.Engine.TransactionManager.HasActiveTransaction(Context.SessionId))
        {
            return work();
        }

        RunControl("BEGIN");
        int result;
        try
        {
            result = work();
        }
        catch
        {
            RunControl("ROLLBACK");
            throw;
        }

        RunControl("COMMIT");
        return result;
    }

    private void RunControl(string command)
    {
        foreach (QueryResult result in Context.Execute(command))
        {
            Annotations.DataVoGeneratedTableSupport.ThrowIfError(result, command);
        }
    }
}

/// <summary>The plan of the query over every row: no condition, no parameters.</summary>
/// <typeparam name="T">The model type.</typeparam>
internal sealed class DataVoTablePlan<T> : DataVoLinqPlan<T, Func<T, bool>>
{
    public static readonly DataVoTablePlan<T> Instance = new();

    private DataVoTablePlan()
        : base(string.Empty, unique: false)
    {
    }

    protected override QueryResult Run(DataVoPreparedStatement statement, Func<T, bool> state, bool borrowed) =>
        borrowed ? statement.ExecuteBorrowed() : statement.Execute();

    protected override DataVoQueryParameter[] Parameters(Func<T, bool> state) => [];
}
