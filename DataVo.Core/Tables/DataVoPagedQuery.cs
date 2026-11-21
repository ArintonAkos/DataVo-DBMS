using System.Collections;
using System.ComponentModel;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.Core;

/// <summary>
/// A window of a table query's rows, after <c>Skip</c> or <c>Take</c>. Nothing is read until the query is enumerated
/// or one of its methods is called.
/// </summary>
/// <remarks>
/// When the filter and any sort keys were compiled, the window becomes <c>LIMIT</c> and <c>OFFSET</c> and the engine
/// stops reading once it is full. Otherwise the same window is cut from the rows in memory. Chained calls narrow
/// one window, with the semantics of <see cref="Enumerable.Skip{TSource}"/> and <see cref="Enumerable.Take{TSource}(IEnumerable{TSource}, int)"/>.
/// Without <c>OrderBy</c> the rows come in the order the query enumerates them; sort first for a stable page.
/// </remarks>
/// <typeparam name="T">The model type.</typeparam>
/// <typeparam name="TState">The type of the value the filter receives.</typeparam>
/// <example>
/// <code>
/// List&lt;Player&gt; page = db.Table&lt;Player&gt;().OrderBy(static p => p.Id).Skip(pageIndex * 20).Take(20).ToList();
/// </code>
/// </example>
public readonly struct DataVoPagedQuery<T, TState> : IEnumerable<T>
{
    private readonly DataVoQuery<T, TState> _source;
    private readonly DataVoQueryShape<T>? _shape;

    internal DataVoPagedQuery(DataVoQuery<T, TState> source, DataVoQueryShape<T> shape)
    {
        _source = source;
        _shape = shape;
    }

    /// <summary>Gets whether the filter and every sort key were compiled, so the engine pages the rows.</summary>
    public bool IsCompiled => _source.IsCompiled && Shape.IsOrderCompiled;

    /// <summary>Gets the statement the query runs, or <see langword="null"/> when it pages in memory.</summary>
    public string? Sql => DataVoComposed.Sql(_source, Shape, _source.Table.ModelColumns);

    private DataVoQueryShape<T> Shape => _shape ?? DataVoQueryShape<T>.None;

    /// <summary>Skips more rows of the window.</summary>
    /// <param name="count">The rows to skip; zero or less skips none.</param>
    /// <returns>The narrower window.</returns>
    public DataVoPagedQuery<T, TState> Skip(int count) => new(_source, Shape.Skipping(count));

    /// <summary>Keeps at most <paramref name="count"/> rows of the window.</summary>
    /// <param name="count">The rows to keep; zero or less keeps none.</param>
    /// <returns>The narrower window.</returns>
    public DataVoPagedQuery<T, TState> Take(int count) => new(_source, Shape.Taking(count));

    /// <summary>Projects each row of the window; see <see cref="DataVoQuery{T, TState}.Select{TResult}"/>.</summary>
    /// <typeparam name="TResult">The type of the projected value.</typeparam>
    /// <param name="selector">Builds the value from a row.</param>
    /// <returns>The projected rows.</returns>
    public DataVoProjectedQuery<T, TState, TResult> Select<TResult>(Func<T, TResult> selector) =>
        new(_source, Shape, null, selector ?? throw new ArgumentNullException(nameof(selector)));

    /// <summary>Projects each row of the window with a compiled projection. Used by generated code.</summary>
    /// <typeparam name="TResult">The type of the projected value.</typeparam>
    /// <param name="projection">The compiled projection.</param>
    /// <param name="selector">The original selector.</param>
    /// <returns>The projected rows.</returns>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public DataVoProjectedQuery<T, TState, TResult> SelectColumns<TResult>(DataVoProjection<T, TResult> projection, Func<T, TResult> selector) =>
        new(_source, Shape, projection, selector ?? throw new ArgumentNullException(nameof(selector)));

    /// <summary>Returns the first row of the window, or the default value when it is empty.</summary>
    /// <returns>The first row, or the default value.</returns>
    public T? FirstOrDefault() => DataVoComposed.First(DataVoComposed.ReadModels(_source, Shape, 1));

    /// <summary>Returns the only row of the window, or the default value when it is empty.</summary>
    /// <returns>The only row, or the default value.</returns>
    /// <exception cref="InvalidOperationException">The window holds more than one row.</exception>
    public T? SingleOrDefault() => DataVoComposed.Single(DataVoComposed.ReadModels(_source, Shape, 2));

    /// <summary>Gets whether the window holds any row.</summary>
    /// <returns><see langword="true"/> when it holds at least one row.</returns>
    public bool Any() => DataVoComposed.Any(_source, Shape);

    /// <summary>Counts the rows of the window. Nothing is sorted and no model is built when the filter is compiled into the scan.</summary>
    /// <returns>The number of rows.</returns>
    public int Count() => DataVoComposed.Count(_source, Shape);

    /// <summary>Reads the window into a list.</summary>
    /// <returns>The rows.</returns>
    public List<T> ToList() => [.. DataVoComposed.ReadModels(_source, Shape, -1)];

    /// <summary>
    /// Registers a strongly-typed reactive subscription observing this paged or top-K window.
    /// Deltas are delivered when <see cref="DataVoContext.DispatchPendingNotifications"/> is called.
    /// </summary>
    /// <param name="onChanged">The callback invoked with typed added, removed, and updated rows.</param>
    /// <returns>A disposable subscription handle.</returns>
    /// <example>
    /// <code>
    /// using var sub = db.Table&lt;Player&gt;()
    ///     .Where(100, static (p, minScore) => p.Score >= minScore)
    ///     .OrderByDescending(static p => p.Score)
    ///     .Take(3)
    ///     .Subscribe(change => Handle(change));
    /// </code>
    /// </example>
    public IDisposable Subscribe(Action<QueryChange<T>> onChanged) =>
        DataVoReactiveLinq.SubscribeModels(_source, Shape, onChanged);

    /// <summary>
    /// Registers a zero-allocation buffered window subscription over this paged or top-K query, writing
    /// the current live window in place into <c>destination.Span[0..count]</c> (clamped to
    /// <c>destination.Length</c>) and invoking <paramref name="onUpdated"/> with <c>count</c>.
    /// </summary>
    /// <param name="destination">Caller-owned buffer receiving the live window.</param>
    /// <param name="onUpdated">Callback invoked with the live row count written whenever the window changes.</param>
    /// <returns>A disposable subscription handle.</returns>
    public IDisposable SubscribeInto(Memory<T> destination, Action<int> onUpdated) =>
        DataVoReactiveLinq.SubscribeModelsInto(_source, Shape, destination, onUpdated);

    /// <summary>Runs the query and enumerates the window.</summary>
    /// <returns>An enumerator over the rows.</returns>
    public IEnumerator<T> GetEnumerator() => DataVoComposed.ReadModels(_source, Shape, -1).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
