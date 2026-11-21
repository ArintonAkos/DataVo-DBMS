using System.Collections;
using System.ComponentModel;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.Core;

/// <summary>
/// The rows of a table query sorted by one or more keys. Like <see cref="DataVoQuery{T, TState}"/>, nothing is read
/// until the query is enumerated or one of its methods is called.
/// </summary>
/// <remarks>
/// When <c>DataVo.Generators</c> compiled every key (each is <c>p =&gt; p.Column</c>) and the filter, the engine sorts
/// and pages the rows: <c>OrderByDescending(p =&gt; p.Score).Take(10)</c> keeps ten rows while it scans instead of
/// sorting the table. Otherwise the filter's rows are sorted in memory by the same keys. Either way the order is the
/// one <c>Enumerable.OrderBy</c> gives: <see cref="Comparer{T}.Default"/> for each key, null first, ties in storage order.
/// </remarks>
/// <typeparam name="T">The model type.</typeparam>
/// <typeparam name="TState">The type of the value the filter receives.</typeparam>
/// <example>
/// <code>
/// List&lt;Player&gt; top = db.Table&lt;Player&gt;()
///     .Where(true, static (p, active) => p.IsActive == active)
///     .OrderByDescending(static p => p.Level)
///     .ThenBy(static p => p.Id)
///     .Take(10)
///     .ToList();
/// </code>
/// </example>
public readonly struct DataVoOrderedQuery<T, TState> : IOrderedEnumerable<T>
{
    private readonly DataVoQuery<T, TState> _source;
    private readonly DataVoQueryShape<T>? _shape;

    internal DataVoOrderedQuery(DataVoQuery<T, TState> source, DataVoQueryShape<T> shape)
    {
        _source = source;
        _shape = shape;
    }

    /// <summary>Gets whether the filter and every sort key were compiled, so the engine sorts the rows.</summary>
    public bool IsCompiled => _source.IsCompiled && Shape.IsOrderCompiled;

    /// <summary>Gets the statement the query runs, or <see langword="null"/> when it sorts in memory.</summary>
    public string? Sql => DataVoComposed.Sql(_source, Shape, _source.Table.ModelColumns);

    private DataVoQueryShape<T> Shape => _shape ?? DataVoQueryShape<T>.None;

    /// <summary>Breaks ties by another key, smallest first.</summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="keySelector">The key, as <c>p =&gt; p.Column</c> to sort in the engine.</param>
    /// <returns>The sorted rows.</returns>
    public DataVoOrderedQuery<T, TState> ThenBy<TKey>(Func<T, TKey> keySelector) => Then(keySelector, null, false, null);

    /// <summary>Breaks ties by another key with a comparer, smallest first. A comparer always sorts in memory.</summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="keySelector">The key.</param>
    /// <param name="comparer">Orders two keys; <see langword="null"/> for the default order.</param>
    /// <returns>The sorted rows.</returns>
    public DataVoOrderedQuery<T, TState> ThenBy<TKey>(Func<T, TKey> keySelector, IComparer<TKey>? comparer) => Then(keySelector, comparer, false, null);

    /// <summary>Breaks ties by another key, largest first.</summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="keySelector">The key, as <c>p =&gt; p.Column</c> to sort in the engine.</param>
    /// <returns>The sorted rows.</returns>
    public DataVoOrderedQuery<T, TState> ThenByDescending<TKey>(Func<T, TKey> keySelector) => Then(keySelector, null, true, null);

    /// <summary>Breaks ties by another key with a comparer, largest first. A comparer always sorts in memory.</summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="keySelector">The key.</param>
    /// <param name="comparer">Orders two keys; <see langword="null"/> for the default order.</param>
    /// <returns>The sorted rows.</returns>
    public DataVoOrderedQuery<T, TState> ThenByDescending<TKey>(Func<T, TKey> keySelector, IComparer<TKey>? comparer) => Then(keySelector, comparer, true, null);

    /// <summary>Breaks ties by a compiled column. Used by generated code.</summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="key">The compiled column and direction.</param>
    /// <param name="keySelector">The original key, used when the query sorts in memory.</param>
    /// <returns>The sorted rows.</returns>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public DataVoOrderedQuery<T, TState> ThenByColumn<TKey>(DataVoOrderKey key, Func<T, TKey> keySelector) =>
        Then(keySelector, null, (key ?? throw new ArgumentNullException(nameof(key))).Descending, key);

    /// <summary>Skips sorted rows, as <see cref="Enumerable.Skip{TSource}"/> does.</summary>
    /// <param name="count">The rows to skip; zero or less skips none.</param>
    /// <returns>The remaining rows.</returns>
    public DataVoPagedQuery<T, TState> Skip(int count) => new(_source, Shape.Skipping(count));

    /// <summary>Keeps the first sorted rows, as <see cref="Enumerable.Take{TSource}(IEnumerable{TSource}, int)"/> does.</summary>
    /// <param name="count">The rows to keep; zero or less keeps none.</param>
    /// <returns>The kept rows.</returns>
    public DataVoPagedQuery<T, TState> Take(int count) => new(_source, Shape.Taking(count));

    /// <summary>Projects each sorted row; see <see cref="DataVoQuery{T, TState}.Select{TResult}"/>.</summary>
    /// <typeparam name="TResult">The type of the projected value.</typeparam>
    /// <param name="selector">Builds the value from a row.</param>
    /// <returns>The projected rows, in order.</returns>
    public DataVoProjectedQuery<T, TState, TResult> Select<TResult>(Func<T, TResult> selector) =>
        new(_source, Shape, null, selector ?? throw new ArgumentNullException(nameof(selector)));

    /// <summary>Projects each sorted row with a compiled projection. Used by generated code.</summary>
    /// <typeparam name="TResult">The type of the projected value.</typeparam>
    /// <param name="projection">The compiled projection.</param>
    /// <param name="selector">The original selector.</param>
    /// <returns>The projected rows, in order.</returns>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public DataVoProjectedQuery<T, TState, TResult> SelectColumns<TResult>(DataVoProjection<T, TResult> projection, Func<T, TResult> selector) =>
        new(_source, Shape, projection, selector ?? throw new ArgumentNullException(nameof(selector)));

    /// <summary>Returns the first row in order, or the default value when no row matches.</summary>
    /// <returns>The first row, or the default value.</returns>
    public T? FirstOrDefault() => DataVoComposed.First(DataVoComposed.ReadModels(_source, Shape, 1));

    /// <summary>Returns the only matching row, or the default value when none matches.</summary>
    /// <returns>The only row, or the default value.</returns>
    /// <exception cref="InvalidOperationException">More than one row matches.</exception>
    public T? SingleOrDefault() => DataVoComposed.Single(DataVoComposed.ReadModels(_source, Shape, 2));

    /// <summary>Gets whether any row matches. Nothing is sorted.</summary>
    /// <returns><see langword="true"/> when at least one row matches.</returns>
    public bool Any() => DataVoComposed.Any(_source, Shape);

    /// <summary>Counts the matching rows. Nothing is sorted.</summary>
    /// <returns>The number of rows.</returns>
    public int Count() => DataVoComposed.Count(_source, Shape);

    /// <summary>Reads the sorted rows into a list.</summary>
    /// <returns>The rows, in order.</returns>
    public List<T> ToList() => [.. DataVoComposed.ReadModels(_source, Shape, -1)];

    /// <summary>
    /// Registers a strongly-typed reactive subscription observing the sorted query results.
    /// Deltas are delivered when <see cref="DataVoContext.DispatchPendingNotifications"/> is called.
    /// </summary>
    /// <param name="onChanged">The callback invoked with typed added, removed, and updated rows.</param>
    /// <returns>A disposable subscription handle.</returns>
    public IDisposable Subscribe(Action<QueryChange<T>> onChanged) =>
        DataVoReactiveLinq.SubscribeModels(_source, Shape, onChanged);

    /// <summary>
    /// Registers a zero-allocation buffered window subscription over the sorted rows, writing up to
    /// <c>destination.Length</c> live rows in sorted order into <paramref name="destination"/> on each drain.
    /// </summary>
    /// <param name="destination">Caller-owned buffer receiving the live sorted window.</param>
    /// <param name="onUpdated">Callback invoked with the live row count written whenever the window changes.</param>
    /// <returns>A disposable subscription handle.</returns>
    public IDisposable SubscribeInto(Memory<T> destination, Action<int> onUpdated) =>
        DataVoReactiveLinq.SubscribeModelsInto(_source, Shape, destination, onUpdated);

    /// <summary>Runs the query and enumerates the sorted rows.</summary>
    /// <returns>An enumerator over the rows, in order.</returns>
    public IEnumerator<T> GetEnumerator() => DataVoComposed.ReadModels(_source, Shape, -1).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <summary>Lets <c>Enumerable.ThenBy</c> on this query add a key, which then sorts in memory.</summary>
    IOrderedEnumerable<T> IOrderedEnumerable<T>.CreateOrderedEnumerable<TKey>(Func<T, TKey> keySelector, IComparer<TKey>? comparer, bool descending) =>
        Then(keySelector, comparer, descending, null);

    private DataVoOrderedQuery<T, TState> Then<TKey>(Func<T, TKey> keySelector, IComparer<TKey>? comparer, bool descending, DataVoOrderKey? column) =>
        new(_source, Shape.ThenBy(new DataVoSortKey<T, TKey>(keySelector ?? throw new ArgumentNullException(nameof(keySelector)), comparer, descending, column)));
}
