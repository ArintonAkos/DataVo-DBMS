using System.Collections;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.Core;

/// <summary>
/// The values a <c>Select</c> builds from a table query's rows. Nothing is read until the query is enumerated or one
/// of its methods is called.
/// </summary>
/// <remarks>
/// <para>
/// When <c>DataVo.Generators</c> compiled the selector, the statement selects only the columns the selector reads
/// (the engine still reads the filter's and sort's columns to test and order rows) and the values are built from
/// them directly: no model is built, so <c>Select(p =&gt; p.Name)</c> over a wide row decodes one column.
/// </para>
/// <para>
/// When the selector was not compiled, or the query sorts in memory, models are read and the selector runs on each,
/// exactly as <c>Enumerable.Select</c> would.
/// </para>
/// </remarks>
/// <typeparam name="T">The model type.</typeparam>
/// <typeparam name="TState">The type of the value the filter receives.</typeparam>
/// <typeparam name="TResult">The type of the projected value.</typeparam>
/// <example>
/// <code>
/// var board = db.Table&lt;Player&gt;()
///     .OrderByDescending(static p => p.Score)
///     .Take(10)
///     .Select(static p => new { p.Name, p.Score })
///     .ToList();
/// </code>
/// </example>
public readonly struct DataVoProjectedQuery<T, TState, TResult> : IEnumerable<TResult>
{
    private readonly DataVoQuery<T, TState> _source;
    private readonly DataVoQueryShape<T>? _shape;
    private readonly DataVoProjection<T, TResult>? _projection;
    private readonly Func<T, TResult> _selector;

    internal DataVoProjectedQuery(DataVoQuery<T, TState> source, DataVoQueryShape<T> shape, DataVoProjection<T, TResult>? projection, Func<T, TResult> selector)
    {
        _source = source;
        _shape = shape;
        _projection = projection;
        _selector = selector;
    }

    /// <summary>
    /// Gets whether the filter, every sort key and the selector were compiled, so the engine reads only the
    /// projected columns and no model is built.
    /// </summary>
    public bool IsCompiled => _projection is not null && _source.IsCompiled && Shape.IsOrderCompiled;

    /// <summary>Gets the statement the query runs, or <see langword="null"/> when it runs in memory.</summary>
    public string? Sql => DataVoComposed.Sql(_source, Shape, _projection?.ColumnList ?? _source.Table.ModelColumns);

    private DataVoQueryShape<T> Shape => _shape ?? DataVoQueryShape<T>.None;

    /// <summary>Skips projected values, as <see cref="Enumerable.Skip{TSource}"/> does.</summary>
    /// <param name="count">The values to skip; zero or less skips none.</param>
    /// <returns>The remaining values.</returns>
    public DataVoProjectedQuery<T, TState, TResult> Skip(int count) => new(_source, Shape.Skipping(count), _projection, _selector);

    /// <summary>Keeps the first projected values, as <see cref="Enumerable.Take{TSource}(IEnumerable{TSource}, int)"/> does.</summary>
    /// <param name="count">The values to keep; zero or less keeps none.</param>
    /// <returns>The kept values.</returns>
    public DataVoProjectedQuery<T, TState, TResult> Take(int count) => new(_source, Shape.Taking(count), _projection, _selector);

    /// <summary>Returns the first value, or the default value when there is none.</summary>
    /// <returns>The first value, or the default value.</returns>
    public TResult? FirstOrDefault() => DataVoComposed.First(Read(1));

    /// <summary>Returns the only value, or the default value when there is none.</summary>
    /// <returns>The only value, or the default value.</returns>
    /// <exception cref="InvalidOperationException">There is more than one value.</exception>
    public TResult? SingleOrDefault() => DataVoComposed.Single(Read(2));

    /// <summary>Gets whether there is any value. The selector is not run.</summary>
    /// <returns><see langword="true"/> when at least one row matches.</returns>
    public bool Any() => DataVoComposed.Any(_source, Shape);

    /// <summary>Counts the values. The selector is not run.</summary>
    /// <returns>The number of values.</returns>
    public int Count() => DataVoComposed.Count(_source, Shape);

    /// <summary>Reads the values into a list.</summary>
    /// <returns>The values, in order.</returns>
    public List<TResult> ToList() => [.. Read(-1)];

    /// <summary>
    /// Registers a strongly-typed reactive subscription delivering projected <see cref="QueryChange{TResult}"/>
    /// deltas when <see cref="DataVoContext.DispatchPendingNotifications"/> is called.
    /// </summary>
    /// <param name="onChanged">The callback invoked with projected added, removed, and updated items.</param>
    /// <returns>A disposable subscription handle.</returns>
    public IDisposable Subscribe(Action<QueryChange<TResult>> onChanged) =>
        DataVoReactiveLinq.SubscribeProjected(
            _source,
            Shape,
            _projection,
            _selector ?? throw new InvalidOperationException("This query was not created by a DataVoTable."),
            onChanged);

    /// <summary>
    /// Registers a zero-allocation buffered window subscription over this projected query. When compiled by
    /// <c>DataVo.Generators</c>, DataVo projects live rows through a generated value-type
    /// <see cref="IReactiveRowProjector{TProjection}"/> directly into <c>destination.Span[0..count]</c>
    /// (clamped to <c>destination.Length</c>) and invokes <paramref name="onUpdated"/> with zero heap allocations.
    /// </summary>
    /// <param name="destination">Caller-owned buffer receiving the live projected window.</param>
    /// <param name="onUpdated">Callback invoked with the number of projected items written whenever the window changes.</param>
    /// <returns>A disposable subscription handle.</returns>
    /// <example>
    /// <code>
    /// var slots = new HudSlot[3];
    /// using var sub = db.Table&lt;Player&gt;()
    ///     .OrderByDescending(static p => p.Score)
    ///     .Take(3)
    ///     .Select(static p => new HudSlot(p.Id, p.Score))
    ///     .SubscribeInto(slots.AsMemory(), count => RenderHud(slots.AsSpan(0, count)));
    /// </code>
    /// </example>
    public IDisposable SubscribeInto(Memory<TResult> destination, Action<int> onUpdated) =>
        DataVoReactiveLinq.SubscribeProjectedInto(
            _source,
            Shape,
            _projection,
            _selector ?? throw new InvalidOperationException("This query was not created by a DataVoTable."),
            destination,
            onUpdated);

    /// <summary>Runs the query and enumerates the values. Rows are streamed when the query is not sorted.</summary>
    /// <returns>An enumerator over the values.</returns>
    public IEnumerator<TResult> GetEnumerator() => Read(-1).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    private IEnumerable<TResult> Read(long cap) =>
        DataVoComposed.ReadProjected(_source, Shape, _projection, _selector ?? throw new InvalidOperationException("This query was not created by a DataVoTable."), cap);
}
