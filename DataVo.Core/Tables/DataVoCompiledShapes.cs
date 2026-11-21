using System.ComponentModel;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.Core;

/// <summary>
/// A sort key that <c>DataVo.Generators</c> compiled: one column of the model, sorted up or down. Generated code
/// creates one per <c>OrderBy</c> or <c>ThenBy</c> call site; application code does not use this type.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed class DataVoOrderKey
{
    /// <summary>Describes a compiled sort key.</summary>
    /// <param name="column">The column rows are sorted by.</param>
    /// <param name="descending">Whether rows are sorted from largest to smallest.</param>
    public DataVoOrderKey(string column, bool descending)
    {
        Column = column ?? throw new ArgumentNullException(nameof(column));
        Descending = descending;
        Sql = descending ? column + " DESC" : column;
    }

    /// <summary>Gets the column rows are sorted by.</summary>
    public string Column { get; }

    /// <summary>Gets whether rows are sorted from largest to smallest.</summary>
    public bool Descending { get; }

    /// <summary>Gets the key as an ORDER BY entry.</summary>
    internal string Sql { get; }
}

/// <summary>
/// A <c>Select</c> projection that <c>DataVo.Generators</c> compiled: the columns it reads, and the code that builds
/// its result from those columns alone, without building the model. Generated code creates one per call site.
/// </summary>
/// <typeparam name="T">The model type.</typeparam>
/// <typeparam name="TResult">The type of the projected value.</typeparam>
[EditorBrowsable(EditorBrowsableState.Never)]
public class DataVoProjection<T, TResult>
{
    private readonly string[] _columns;

    /// <summary>Describes a compiled projection.</summary>
    /// <param name="columns">The columns the projection reads; at least one.</param>
    /// <param name="read">Builds the projected value from a row holding <paramref name="columns"/>.</param>
    /// <exception cref="ArgumentException"><paramref name="columns"/> is empty.</exception>
    public DataVoProjection(string[] columns, Func<DataVoRow, TResult> read)
    {
        if (columns is null || columns.Length == 0)
        {
            throw new ArgumentException("A projection reads at least one column.", nameof(columns));
        }

        _columns = columns;
        Columns = columns;
        ColumnList = string.Join(", ", columns);
        Read = read ?? throw new ArgumentNullException(nameof(read));
    }

    /// <summary>
    /// Creates a compiled projection that also carries a zero-allocation value-type
    /// <see cref="IReactiveRowProjector{TProjection}"/> for reactive <c>Subscribe</c> and <c>SubscribeInto</c> calls.
    /// </summary>
    /// <typeparam name="TProjector">The generated struct projector type.</typeparam>
    /// <param name="columns">The columns the projection reads; at least one.</param>
    /// <param name="read">Builds the projected value from a streamed <see cref="DataVoRow"/>.</param>
    /// <param name="projector">The value-type projector mapping borrowed reactive rows into <typeparamref name="TResult"/>.</param>
    /// <returns>A compiled projection supporting both batch streaming and zero-allocation reactive delivery.</returns>
    public static DataVoProjection<T, TResult> Create<TProjector>(
        string[] columns,
        Func<DataVoRow, TResult> read,
        TProjector projector)
        where TProjector : struct, IReactiveRowProjector<TResult>
        => new ReactiveDataVoProjection<T, TResult, TProjector>(columns, read, projector);

    /// <summary>Gets the columns the projection reads.</summary>
    public IReadOnlyList<string> Columns { get; }

    /// <summary>Gets the backing column array in projection ordinal order.</summary>
    internal string[] ColumnArray => _columns;

    /// <summary>Gets the columns as a SELECT list.</summary>
    internal string ColumnList { get; }

    /// <summary>Gets the code that builds the projected value from a row.</summary>
    internal Func<DataVoRow, TResult> Read { get; }

    /// <summary>Gets whether this projection carries a compiled zero-allocation reactive projector.</summary>
    internal virtual bool HasReactiveProjector => false;

    /// <summary>Projects a dictionary row returned by the owned reactive lane into <typeparamref name="TResult"/>.</summary>
    internal virtual TResult ReadDictionary(IReadOnlyDictionary<string, object?> row)
    {
        var schema = new DataVoRowSchema(_columns);
        var values = new object?[_columns.Length];
        for (int i = 0; i < _columns.Length; i++)
        {
            values[i] = row.TryGetValue(_columns[i], out object? value) ? value : null;
        }

        return Read(new DataVoRow(schema, values));
    }

    /// <summary>Subscribes on the borrowed reactive lane when supported, projecting directly from <see cref="Runtime.Reactive.RowRef"/>.</summary>
    internal virtual IDisposable SubscribeBorrowed(
        DataVoContext context,
        DataVoPreparedStatement statement,
        DataVoQueryParameter[] parameters,
        Action<QueryChange<TResult>> onChanged)
    {
        return context.Subscribe(
            statement,
            change => DataVoReactiveLinq.DeliverOwned(change, ReadDictionary, onChanged),
            parameters);
    }

    /// <summary>Subscribes a zero-allocation buffered window into <paramref name="destination"/>.</summary>
    internal virtual IDisposable SubscribeInto(
        DataVoContext context,
        string tableName,
        string primaryKeyColumn,
        IReadOnlyList<BufferedWindowSortKey> sortKeys,
        int skip,
        int take,
        ReactivePredicate? predicate,
        Func<IReadOnlyDictionary<string, object?>, bool>? fallbackPredicate,
        Memory<TResult> destination,
        Action<int> onUpdated)
    {
        var projector = new DictionaryRowRefProjector<TResult>(this);
        var query = new BufferedWindowReactiveQuery<TResult, DictionaryRowRefProjector<TResult>>(
            tableName,
            primaryKeyColumn,
            _columns,
            sortKeys,
            skip,
            take,
            predicate,
            fallbackPredicate,
            projector,
            destination,
            onUpdated);
        return context.Engine.Reactive.AddDirect(context, query);
    }

    private readonly struct DictionaryRowRefProjector<TOut>(DataVoProjection<T, TOut> owner) : IReactiveRowProjector<TOut>
    {
        public TOut Project(in Runtime.Reactive.RowRef row) => owner.ReadDictionary(row.ToOwnedDictionary());
    }
}

/// <summary>
/// A compiled <see cref="DataVoProjection{T, TResult}"/> backed by a generated value-type
/// <see cref="IReactiveRowProjector{TProjection}"/> struct for zero-allocation reactive subscriptions.
/// </summary>
internal sealed class ReactiveDataVoProjection<T, TResult, TProjector> : DataVoProjection<T, TResult>
    where TProjector : struct, IReactiveRowProjector<TResult>
{
    private readonly TProjector _projector;
    private readonly ReactiveRowSchema _reactiveSchema;

    public ReactiveDataVoProjection(string[] columns, Func<DataVoRow, TResult> read, TProjector projector)
        : base(columns, read)
    {
        _projector = projector;
        _reactiveSchema = new ReactiveRowSchema(columns);
    }

    internal override bool HasReactiveProjector => true;

    internal override TResult ReadDictionary(IReadOnlyDictionary<string, object?> row)
    {
        string[] columns = ColumnArray;
        var cells = new CellValue[columns.Length];
        for (int i = 0; i < columns.Length; i++)
        {
            cells[i] = row.TryGetValue(columns[i], out object? value)
                ? CellValue.FromBorrowedObject(value)
                : CellValue.Null;
        }

        var rowRef = new Runtime.Reactive.RowRef(_reactiveSchema, cells);
        return _projector.Project(in rowRef);
    }

    internal override IDisposable SubscribeBorrowed(
        DataVoContext context,
        DataVoPreparedStatement statement,
        DataVoQueryParameter[] parameters,
        Action<QueryChange<TResult>> onChanged)
    {
        TProjector projector = _projector;
        return context.SubscribeZeroAlloc(
            statement,
            (in QueryChangeRef delta) => DataVoReactiveLinq.DeliverBorrowed(in delta, projector, onChanged),
            parameters);
    }

    internal override IDisposable SubscribeInto(
        DataVoContext context,
        string tableName,
        string primaryKeyColumn,
        IReadOnlyList<BufferedWindowSortKey> sortKeys,
        int skip,
        int take,
        ReactivePredicate? predicate,
        Func<IReadOnlyDictionary<string, object?>, bool>? fallbackPredicate,
        Memory<TResult> destination,
        Action<int> onUpdated)
    {
        var query = new BufferedWindowReactiveQuery<TResult, TProjector>(
            tableName,
            primaryKeyColumn,
            ColumnArray,
            sortKeys,
            skip,
            take,
            predicate,
            fallbackPredicate,
            _projector,
            destination,
            onUpdated);
        return context.Engine.Reactive.AddDirect(context, query);
    }
}

/// <summary>
/// Describes the columns an <c>ExecuteUpdate</c> call sets. Write it as a lambda that chains <see cref="Set{TValue}(Func{T, TValue}, TValue)"/>
/// calls; <c>DataVo.Generators</c> reads the lambda at build time and turns it into the SET list of one
/// <c>UPDATE</c> statement. The lambda itself is never run.
/// </summary>
/// <typeparam name="T">The model type.</typeparam>
/// <example>
/// <code>
/// int changed = db.Table&lt;Player&gt;()
///     .Where(10, static (p, min) => p.Level >= min)
///     .ExecuteUpdate(100L, static (set, bonus) => set
///         .Set(p => p.Gold, p => p.Gold + bonus)
///         .SetNull(p => p.Guild));
/// </code>
/// </example>
public sealed class DataVoSetters<T>
{
    private DataVoSetters()
    {
    }

    /// <summary>Sets a column to a value.</summary>
    /// <typeparam name="TValue">The column's type.</typeparam>
    /// <param name="column">The column, as <c>p =&gt; p.Column</c>.</param>
    /// <param name="value">The new value: a literal, the state value or a field or property of it, or a static member.</param>
    /// <returns>The same setters, to chain another call.</returns>
    /// <exception cref="NotSupportedException">Always, when called: setters only describe a compiled update.</exception>
    public DataVoSetters<T> Set<TValue>(Func<T, TValue> column, TValue value) => throw NotRun();

    /// <summary>Sets a column to a value computed from the row's own columns, such as <c>p =&gt; p.Gold + bonus</c>.</summary>
    /// <typeparam name="TValue">The column's type.</typeparam>
    /// <param name="column">The column, as <c>p =&gt; p.Column</c>.</param>
    /// <param name="value">The new value: columns and values joined by <c>+</c>, <c>-</c> and <c>*</c>.</param>
    /// <returns>The same setters, to chain another call.</returns>
    /// <exception cref="NotSupportedException">Always, when called: setters only describe a compiled update.</exception>
    public DataVoSetters<T> Set<TValue>(Func<T, TValue> column, Func<T, TValue> value) => throw NotRun();

    /// <summary>
    /// Sets a nullable column to null. <c>Set(p =&gt; p.Column, null)</c> cannot choose between the two
    /// <c>Set</c> overloads, so a literal null is written with this method.
    /// </summary>
    /// <typeparam name="TValue">The column's type.</typeparam>
    /// <param name="column">The column, as <c>p =&gt; p.Column</c>.</param>
    /// <returns>The same setters, to chain another call.</returns>
    /// <exception cref="NotSupportedException">Always, when called: setters only describe a compiled update.</exception>
    public DataVoSetters<T> SetNull<TValue>(Func<T, TValue> column) => throw NotRun();

    private static NotSupportedException NotRun() =>
        new("DataVoSetters only describe an ExecuteUpdate call; DataVo.Generators compiles them at build time and they are never run.");
}
