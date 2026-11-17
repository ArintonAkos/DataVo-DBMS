using System.ComponentModel;

namespace DataVo.Core;

/// <summary>
/// Sorting, paging, projection and bulk changes over the rows a query's filter matches.
/// </summary>
public readonly partial struct DataVoQuery<T, TState>
{
    private const string UpdateNotCompiled =
        "ExecuteUpdate runs only when DataVo.Generators compiles it at build time, and this call was not compiled. " +
        "Use a C# 12 or newer compiler with <InterceptorsNamespaces>$(InterceptorsNamespaces);DataVo.Generated</InterceptorsNamespaces> " +
        "in the project and fix any DATAVOL005 error the build reports. On an older compiler, such as Unity's, run a prepared UPDATE statement instead.";

    /// <summary>Sorts the matching rows by a key, smallest first. Ties keep the order the query enumerates them in.</summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="keySelector">The key, as <c>p =&gt; p.Column</c> so the engine sorts.</param>
    /// <returns>The sorted rows; chain <c>ThenBy</c>, <c>Skip</c>, <c>Take</c> or <c>Select</c>.</returns>
    /// <example>
    /// <code>
    /// Player? weakest = db.Table&lt;Player&gt;().Where(static p => p.IsActive).OrderBy(static p => p.Level).FirstOrDefault();
    /// </code>
    /// </example>
    public DataVoOrderedQuery<T, TState> OrderBy<TKey>(Func<T, TKey> keySelector) => Order(keySelector, null, false, null);

    /// <summary>Sorts the matching rows by a key with a comparer, smallest first. A comparer always sorts in memory.</summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="keySelector">The key.</param>
    /// <param name="comparer">Orders two keys; <see langword="null"/> for the default order.</param>
    /// <returns>The sorted rows.</returns>
    public DataVoOrderedQuery<T, TState> OrderBy<TKey>(Func<T, TKey> keySelector, IComparer<TKey>? comparer) => Order(keySelector, comparer, false, null);

    /// <summary>Sorts the matching rows by a key, largest first. Ties keep the order the query enumerates them in.</summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="keySelector">The key, as <c>p =&gt; p.Column</c> so the engine sorts.</param>
    /// <returns>The sorted rows.</returns>
    public DataVoOrderedQuery<T, TState> OrderByDescending<TKey>(Func<T, TKey> keySelector) => Order(keySelector, null, true, null);

    /// <summary>Sorts the matching rows by a key with a comparer, largest first. A comparer always sorts in memory.</summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="keySelector">The key.</param>
    /// <param name="comparer">Orders two keys; <see langword="null"/> for the default order.</param>
    /// <returns>The sorted rows.</returns>
    public DataVoOrderedQuery<T, TState> OrderByDescending<TKey>(Func<T, TKey> keySelector, IComparer<TKey>? comparer) => Order(keySelector, comparer, true, null);

    /// <summary>Sorts the matching rows by a compiled column. Used by generated code.</summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="key">The compiled column and direction.</param>
    /// <param name="keySelector">The original key, used when the query sorts in memory.</param>
    /// <returns>The sorted rows.</returns>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public DataVoOrderedQuery<T, TState> OrderByColumn<TKey>(DataVoOrderKey key, Func<T, TKey> keySelector) =>
        Order(keySelector, null, (key ?? throw new ArgumentNullException(nameof(key))).Descending, key);

    /// <summary>
    /// Skips matching rows, as <see cref="Enumerable.Skip{TSource}"/> does. When the filter is compiled the engine
    /// skips them; rows come in the order the query enumerates them in.
    /// </summary>
    /// <param name="count">The rows to skip; zero or less skips none.</param>
    /// <returns>The remaining rows.</returns>
    public DataVoPagedQuery<T, TState> Skip(int count) => new(this, DataVoQueryShape<T>.None.Skipping(count));

    /// <summary>
    /// Keeps the first matching rows, as <see cref="Enumerable.Take{TSource}(IEnumerable{TSource}, int)"/> does. When
    /// the filter is compiled the engine stops reading once it has them.
    /// </summary>
    /// <param name="count">The rows to keep; zero or less keeps none.</param>
    /// <returns>The kept rows.</returns>
    public DataVoPagedQuery<T, TState> Take(int count) => new(this, DataVoQueryShape<T>.None.Taking(count));

    /// <summary>
    /// Projects each matching row. When <c>DataVo.Generators</c> compiles <paramref name="selector"/>, the statement
    /// selects only the columns it reads and no model is built; otherwise each model is read and projected.
    /// </summary>
    /// <typeparam name="TResult">The type of the projected value.</typeparam>
    /// <param name="selector">
    /// Builds the value from a row's columns: a column, a tuple, an anonymous type, a constructor or object
    /// initializer, or any expression over columns, constants and static members.
    /// </param>
    /// <returns>The projected values.</returns>
    /// <example>
    /// <code>
    /// var names = db.Table&lt;Player&gt;().Where(10, static (p, min) => p.Level >= min).Select(static p => new PlayerName(p.Id, p.Name)).ToList();
    /// </code>
    /// </example>
    public DataVoProjectedQuery<T, TState, TResult> Select<TResult>(Func<T, TResult> selector) =>
        new(this, DataVoQueryShape<T>.None, null, selector ?? throw new ArgumentNullException(nameof(selector)));

    /// <summary>Projects each matching row with a compiled projection. Used by generated code.</summary>
    /// <typeparam name="TResult">The type of the projected value.</typeparam>
    /// <param name="projection">The compiled projection.</param>
    /// <param name="selector">The original selector.</param>
    /// <returns>The projected values.</returns>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public DataVoProjectedQuery<T, TState, TResult> SelectColumns<TResult>(DataVoProjection<T, TResult> projection, Func<T, TResult> selector) =>
        new(this, DataVoQueryShape<T>.None, projection ?? throw new ArgumentNullException(nameof(projection)), selector ?? throw new ArgumentNullException(nameof(selector)));

    /// <summary>
    /// Deletes the matching rows. A compiled filter becomes one <c>DELETE ... WHERE</c> statement; otherwise the
    /// matching rows' keys are read first and deleted one by one inside a single transaction.
    /// </summary>
    /// <returns>The number of rows deleted.</returns>
    /// <remarks>Inside an open transaction the delete is part of it, and a rollback undoes it.</remarks>
    /// <example>
    /// <code>
    /// int removed = db.Table&lt;Player&gt;().Where(0, static (p, none) => p.Level == none).ExecuteDelete();
    /// </code>
    /// </example>
    public int ExecuteDelete()
    {
        DataVoTable<T> table = Table;
        if (TryBind(out string? where, out DataVoQueryParameter[] parameters))
        {
            return table.Change(DataVoSql.Delete(table.TableName, where), parameters);
        }

        return table.ChangeEachKey(MatchingKeys(), DataVoSql.Delete(table.TableName, DataVoSql.KeyEquals(table.KeyColumn)), []);
    }

    /// <summary>
    /// Sets columns of the matching rows with one <c>UPDATE</c> statement. <c>DataVo.Generators</c> must compile the
    /// call; values the setters read come from literals and static members.
    /// </summary>
    /// <param name="setters">The columns to set, as <c>set =&gt; set.Set(p =&gt; p.Column, value)</c> calls.</param>
    /// <returns>The number of rows updated.</returns>
    /// <exception cref="NotSupportedException">The call was not compiled.</exception>
    public int ExecuteUpdate(Func<DataVoSetters<T>, DataVoSetters<T>> setters) => throw new NotSupportedException(UpdateNotCompiled);

    /// <summary>
    /// Sets columns of the matching rows from a state value, with one <c>UPDATE</c> statement. <c>DataVo.Generators</c>
    /// must compile the call. Pass the values as state and mark the lambda <see langword="static"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the state value.</typeparam>
    /// <param name="value">The value the setters read.</param>
    /// <param name="setters">The columns to set: a value, or an expression over the row's columns with <c>+</c>, <c>-</c> and <c>*</c>.</param>
    /// <returns>The number of rows updated.</returns>
    /// <exception cref="NotSupportedException">The call was not compiled.</exception>
    /// <example>
    /// <code>
    /// int paid = db.Table&lt;Player&gt;()
    ///     .Where(static p => p.IsActive)
    ///     .ExecuteUpdate(250L, static (set, reward) => set.Set(p => p.Gold, p => p.Gold + reward));
    /// </code>
    /// </example>
    public int ExecuteUpdate<TValue>(TValue value, Func<DataVoSetters<T>, TValue, DataVoSetters<T>> setters) => throw new NotSupportedException(UpdateNotCompiled);

    /// <summary>
    /// Runs a compiled update over the matching rows. A compiled filter becomes the statement's WHERE; otherwise the
    /// matching rows' keys are read first and updated one by one inside a single transaction. Used by generated code.
    /// </summary>
    /// <param name="assignments">The SET list over <c>@u0</c>, <c>@u1</c>...</param>
    /// <param name="values">The values of the SET list.</param>
    /// <returns>The number of rows updated.</returns>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public int ExecuteUpdateSet(string assignments, DataVoQueryParameter[] values)
    {
        DataVoTable<T> table = Table;
        if (TryBind(out string? where, out DataVoQueryParameter[] parameters))
        {
            return table.Change(DataVoSql.Update(table.TableName, assignments, where), DataVoSql.Concat(parameters, values));
        }

        return table.ChangeEachKey(MatchingKeys(), DataVoSql.Update(table.TableName, assignments, DataVoSql.KeyEquals(table.KeyColumn)), values);
    }

    /// <summary>Gets the filter as SQL and its values, when the plan was compiled and binds this state.</summary>
    /// <param name="where">The condition, or <see langword="null"/> for every row.</param>
    /// <param name="parameters">The condition's values.</param>
    /// <returns><see langword="false"/> when the filter runs as C# instead.</returns>
    internal bool TryBind(out string? where, out DataVoQueryParameter[] parameters)
    {
        if (_plan is not null && _plan.Binds(_state))
        {
            where = _plan.Condition;
            parameters = _plan.Bind(_state);
            return true;
        }

        where = null;
        parameters = [];
        return false;
    }

    /// <summary>Counts the matching rows up to <paramref name="limit"/> (0 for all), without building them when the scan can test them.</summary>
    internal int CountUpTo(int limit)
    {
        if (_plan is not null && _plan.Binds(_state) && _plan.TryCountMatching(Table, _state, limit, out int matching))
        {
            return matching;
        }

        using Enumerator rows = GetEnumerator();
        return rows.CountRemaining(limit);
    }

    private DataVoOrderedQuery<T, TState> Order<TKey>(Func<T, TKey> keySelector, IComparer<TKey>? comparer, bool descending, DataVoOrderKey? column) =>
        new(this, DataVoQueryShape<T>.None.OrderedBy(new DataVoSortKey<T, TKey>(keySelector ?? throw new ArgumentNullException(nameof(keySelector)), comparer, descending, column)));

    /// <summary>Reads the primary key of each row the predicate accepts, scanning the table.</summary>
    private IEnumerable<object?> MatchingKeys()
    {
        DataVoTable<T> table = Table;
        string key = table.KeyColumn;
        foreach (DataVoRow row in table.Scan())
        {
            if (_predicate(table.Read(row), _state))
            {
                yield return row[key];
            }
        }
    }
}
