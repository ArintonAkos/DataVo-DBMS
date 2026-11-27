using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// A single-table per-group incremental aggregate (L2): <c>GROUP BY</c> with
/// <c>COUNT</c>/<c>SUM</c>/<c>AVG</c>/<c>MIN</c>/<c>MAX</c> and an optional pre-aggregation <c>WHERE</c>.
/// </summary>
/// <remarks>
/// Change images are loaded into the table's catalog layout; a row contributes to its group only while it
/// matches the <c>WHERE</c>. The group state itself lives in the shared <see cref="ReactiveGroupAggregator"/>,
/// keyed by group values, so it is independent of the engine's out-of-place <c>UPDATE</c> row-id reassignment.
/// </remarks>
internal sealed class AggregateReactiveQuery : IBorrowedReactiveQuery
{
    private readonly ReactivePredicate _predicate;
    private readonly ReactiveRowLayout _layout;
    private readonly ReactiveGroupAggregator _aggregator;
    private readonly CellValue[] _image;

    /// <summary>
    /// Compiles the supplied parsed SELECT into an incremental aggregate operator.
    /// </summary>
    /// <param name="select">The parsed aggregate SELECT.</param>
    /// <param name="engine">The owning engine (catalog access for the table's columns).</param>
    /// <param name="databaseName">The database that owns the source table.</param>
    /// <exception cref="NotSupportedException">The query is outside the supported aggregate subset.</exception>
    public AggregateReactiveQuery(SelectStatement select, DataVoEngine engine, string databaseName)
    {
        Validate(select);

        Table = select.FromTable!.Name;
        string alias = select.FromAlias?.Name ?? Table;
        _predicate = ReactivePredicate.Compile(select.WhereExpression);
        _layout = ReactiveTableLayouts.ForTable(engine, Table, databaseName);
        _image = new CellValue[_layout.Count];
        _aggregator = new ReactiveGroupAggregator(select, (qualifier, column) =>
            ReactiveTableLayouts.RequireOrdinal(_layout, Table, alias, qualifier, column));
    }

    /// <summary>Gets the source table this aggregate observes.</summary>
    public string Table { get; }

    /// <inheritdoc />
    public IReadOnlyCollection<string> Tables => [Table];

    /// <inheritdoc />
    public ReactiveRowSchema OutputSchema => _aggregator.OutputSchema;

    /// <inheritdoc />
    public void Seed(string table, IEnumerable<TypedRow> rows)
    {
        foreach (TypedRow row in rows)
        {
            if (_layout.TryLoad(row, _image) && Matches())
            {
                _aggregator.Add(_image);
            }
        }

        _aggregator.DiscardTouched();
    }

    /// <inheritdoc />
    public void ApplyInto(IReadOnlyList<RowChange> changes, QueryChangeBuilder builder)
    {
        // Index loop (not foreach) so iterating the IReadOnlyList does not allocate a boxed enumerator.
        for (int i = 0; i < changes.Count; i++)
        {
            RowChange change = changes[i];
            if (_layout.TryLoadBefore(change, _image) && Matches())
            {
                _aggregator.Remove(_image);
            }

            if (_layout.TryLoadAfter(change, _image) && Matches())
            {
                _aggregator.Add(_image);
            }
        }

        _aggregator.FlushInto(builder);
    }

    private bool Matches() => _predicate.Matches(new RowRef(_layout.Schema, _image));

    private static void Validate(SelectStatement select)
    {
        if (select.Joins.Count > 0)
        {
            throw new NotSupportedException("Reactive aggregates do not support JOINs.");
        }

        if (select.Ctes.Count > 0)
        {
            throw new NotSupportedException("Reactive aggregates do not support common table expressions.");
        }

        if (select.IsDistinct)
        {
            throw new NotSupportedException("Reactive aggregates do not support DISTINCT.");
        }

        if (select.HavingExpression is not null)
        {
            throw new NotSupportedException("Reactive aggregates do not support HAVING.");
        }

        if (select.OrderByExpression is not null || select.LimitExpression is not null)
        {
            throw new NotSupportedException("Reactive aggregates do not support ORDER BY / LIMIT.");
        }

        if (select.FromTable is null)
        {
            throw new NotSupportedException("Reactive aggregates require a single FROM table.");
        }

        foreach (SelectColumnNode column in select.Columns)
        {
            if (column.Expression is WindowFunctionExpressionNode)
            {
                throw new NotSupportedException("Reactive aggregates do not support window functions.");
            }
        }
    }
}
