using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// A single-table linear (L1) reactive query: a <c>SELECT … WHERE</c> over comparisons,
/// <c>AND</c>/<c>OR</c>, <c>IS [NOT] NULL</c> and <c>LIKE</c>.
/// </summary>
/// <remarks>
/// The operator is stateless: each committed change is classified by evaluating the predicate over its
/// before and after images (the §4.3 enter/leave/stay matrix), which is robust to the engine's out-of-place
/// <c>UPDATE</c> reassigning row ids. Images are loaded into the table's catalog layout and projected
/// straight into the delta builder, so the steady state allocates nothing. Comparison and NULL semantics are
/// delegated to <see cref="ReactivePredicate"/>, which mirrors the batch single-table WHERE path.
/// </remarks>
internal sealed class ReactiveSubscription : IBorrowedReactiveQuery
{
    private readonly ReactivePredicate _predicate;
    private readonly ReactiveRowLayout _layout;
    private readonly ReactiveProjection _projection;
    private readonly CellValue[] _before;
    private readonly CellValue[] _after;
    private readonly CellValue[] _beforeRow;
    private readonly CellValue[] _afterRow;

    /// <summary>
    /// Compiles a parsed single-table linear SELECT.
    /// </summary>
    /// <param name="select">The parsed SELECT.</param>
    /// <param name="engine">The owning engine (catalog access for the table's columns).</param>
    /// <param name="databaseName">The database that owns the table.</param>
    /// <exception cref="NotSupportedException">
    /// The query uses a construct outside the linear (L1) layer, such as joins, aggregates, <c>GROUP BY</c>,
    /// <c>ORDER BY</c>, <c>LIMIT</c>, <c>DISTINCT</c>, computed projections or subqueries.
    /// </exception>
    public ReactiveSubscription(SelectStatement select, DataVoEngine engine, string databaseName)
    {
        Validate(select);

        Table = select.FromTable!.Name;
        string alias = select.FromAlias?.Name ?? Table;
        _predicate = ReactivePredicate.Compile(select.WhereExpression);
        _layout = ReactiveTableLayouts.ForTable(engine, Table, databaseName);
        _projection = ReactiveTableLayouts.Project(select.Columns, _layout, Table, alias);
        _before = new CellValue[_layout.Count];
        _after = new CellValue[_layout.Count];
        _beforeRow = new CellValue[_projection.Ordinals.Length];
        _afterRow = new CellValue[_projection.Ordinals.Length];
    }

    /// <summary>Gets the source table this subscription observes.</summary>
    public string Table { get; }

    /// <inheritdoc />
    public IReadOnlyCollection<string> Tables => [Table];

    /// <inheritdoc />
    public ReactiveRowSchema OutputSchema => _projection.Schema;

    /// <inheritdoc />
    /// <remarks>The linear operator keeps no state: the seed is the baseline the next changes are diffed from.</remarks>
    public void Seed(string table, IEnumerable<TypedRow> rows)
    {
    }

    /// <summary>
    /// Projects <paramref name="row"/> into <paramref name="destination"/> when it satisfies the predicate.
    /// </summary>
    /// <param name="row">A row of <see cref="Table"/>.</param>
    /// <param name="destination">The output cells; its length must equal the output column count.</param>
    /// <returns><see langword="true"/> when the row is part of the result.</returns>
    public bool TryProject(TypedRow row, Span<CellValue> destination)
    {
        if (!_layout.TryLoad(row, _after) || !Matches(_after))
        {
            return false;
        }

        _projection.Project(_after, destination);
        return true;
    }

    /// <inheritdoc />
    public void ApplyInto(IReadOnlyList<RowChange> changes, QueryChangeBuilder builder)
    {
        for (int i = 0; i < changes.Count; i++)
        {
            RowChange change = changes[i];
            bool beforeMatches = _layout.TryLoadBefore(change, _before) && Matches(_before);
            bool afterMatches = _layout.TryLoadAfter(change, _after) && Matches(_after);

            if (beforeMatches)
            {
                _projection.Project(_before, _beforeRow);
            }

            if (afterMatches)
            {
                _projection.Project(_after, _afterRow);
            }

            if (beforeMatches && afterMatches)
            {
                builder.AddUpdatedBeforeRow(_beforeRow);
                builder.AddUpdatedRow(_afterRow);
            }
            else if (afterMatches)
            {
                builder.AddAddedRow(_afterRow);
            }
            else if (beforeMatches)
            {
                builder.AddRemovedRow(_beforeRow);
            }
        }
    }

    private bool Matches(CellValue[] image) => _predicate.Matches(new RowRef(_layout.Schema, image));

    private static void Validate(SelectStatement select)
    {
        if (select.Joins.Count > 0)
        {
            throw new NotSupportedException("Reactive subscriptions do not support JOINs in the linear (L1) layer.");
        }

        if (select.GroupByExpression is not null || select.HavingExpression is not null)
        {
            throw new NotSupportedException("Reactive subscriptions do not support GROUP BY / HAVING in the linear (L1) layer.");
        }

        if (select.OrderByExpression is not null)
        {
            throw new NotSupportedException("Reactive subscriptions do not support ORDER BY in the linear (L1) layer.");
        }

        if (select.LimitExpression is not null)
        {
            throw new NotSupportedException("Reactive subscriptions do not support LIMIT/OFFSET in the linear (L1) layer.");
        }

        if (select.IsDistinct)
        {
            throw new NotSupportedException("Reactive subscriptions do not support DISTINCT in the linear (L1) layer.");
        }

        if (select.Ctes.Count > 0)
        {
            throw new NotSupportedException("Reactive subscriptions do not support common table expressions in the linear (L1) layer.");
        }

        if (select.FromTable is null)
        {
            throw new NotSupportedException("Reactive subscriptions require a single FROM table.");
        }

        foreach (SelectColumnNode column in select.Columns)
        {
            if (column.Expression is AggregateExpressionNode || column.Expression is WindowFunctionExpressionNode)
            {
                throw new NotSupportedException("Reactive subscriptions do not support aggregate or window functions in the linear (L1) layer.");
            }

            if (column.RawExpression.Contains('(') && !ReactiveTableLayouts.IsStarColumn(column))
            {
                throw new NotSupportedException($"Reactive subscriptions do not support computed projection '{column.RawExpression}' in the linear (L1) layer.");
            }
        }
    }
}
