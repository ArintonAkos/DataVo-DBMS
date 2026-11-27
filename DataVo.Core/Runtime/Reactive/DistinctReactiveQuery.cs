using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// A single-table incremental <c>SELECT DISTINCT</c> operator (L4).
/// </summary>
/// <remarks>
/// DISTINCT is the DBSP <c>distinct</c> operator: a tuple is in the result iff its underlying multiplicity is
/// positive. A committed batch is folded into signed per-tuple deltas (Insert→+1, Delete→−1, Update→−before/+after
/// over the projection) in a <see cref="ReactiveTupleCounter"/>, which emits a tuple as <c>Added</c> only on the
/// <c>0 → &gt;0</c> count transition and as <c>Removed</c> only on <c>&gt;0 → 0</c>. Tuples are keyed by their
/// projected values with SQL value equality, so the state is unaffected by row-id reassignment.
/// </remarks>
internal sealed class DistinctReactiveQuery : IBorrowedReactiveQuery
{
    private readonly ReactivePredicate _predicate;
    private readonly ReactiveRowLayout _layout;
    private readonly ReactiveProjection _projection;
    private readonly ReactiveTupleCounter _tuples;
    private readonly CellValue[] _image;
    private readonly CellValue[] _row;

    /// <summary>
    /// Compiles the supplied parsed <c>SELECT DISTINCT</c> into an incremental distinct operator.
    /// </summary>
    /// <param name="select">The parsed single-table <c>SELECT DISTINCT</c>.</param>
    /// <param name="engine">The owning engine (catalog access for the table's columns).</param>
    /// <param name="databaseName">The database that owns the table.</param>
    /// <exception cref="NotSupportedException">The shape is outside the supported DISTINCT subset (joins, grouping, ordering, computed projections, and so on).</exception>
    public DistinctReactiveQuery(SelectStatement select, DataVoEngine engine, string databaseName)
    {
        Validate(select);

        Table = select.FromTable!.Name;
        string alias = select.FromAlias?.Name ?? Table;
        _predicate = ReactivePredicate.Compile(select.WhereExpression);
        _layout = ReactiveTableLayouts.ForTable(engine, Table, databaseName);
        _projection = ReactiveTableLayouts.Project(select.Columns, _layout, Table, alias);
        _tuples = new ReactiveTupleCounter(_projection.Schema.ColumnCount, isSet: true);
        _image = new CellValue[_layout.Count];
        _row = new CellValue[_projection.Ordinals.Length];
    }

    /// <summary>Gets the source table this distinct operator observes.</summary>
    public string Table { get; }

    /// <inheritdoc />
    public IReadOnlyCollection<string> Tables => [Table];

    /// <inheritdoc />
    public ReactiveRowSchema OutputSchema => _projection.Schema;

    /// <inheritdoc />
    public void Seed(string table, IEnumerable<TypedRow> rows)
    {
        foreach (TypedRow row in rows)
        {
            if (TryProject(_layout.TryLoad(row, _image)))
            {
                _tuples.Seed(_row);
            }
        }
    }

    /// <inheritdoc />
    public void ApplyInto(IReadOnlyList<RowChange> changes, QueryChangeBuilder builder)
    {
        for (int i = 0; i < changes.Count; i++)
        {
            RowChange change = changes[i];
            if (TryProject(_layout.TryLoadBefore(change, _image)))
            {
                _tuples.Add(_row, -1);
            }

            if (TryProject(_layout.TryLoadAfter(change, _image)))
            {
                _tuples.Add(_row, +1);
            }
        }

        _tuples.FlushInto(builder);
    }

    // Projects the loaded image into the row scratch when it was loaded and matches the predicate.
    private bool TryProject(bool loaded)
    {
        if (!loaded || !_predicate.Matches(new RowRef(_layout.Schema, _image)))
        {
            return false;
        }

        _projection.Project(_image, _row);
        return true;
    }

    private static void Validate(SelectStatement select)
    {
        if (!select.IsDistinct)
        {
            throw new NotSupportedException("Reactive DISTINCT requires SELECT DISTINCT.");
        }

        if (select.FromTable is null)
        {
            throw new NotSupportedException("Reactive DISTINCT requires a single FROM table.");
        }

        if (select.Joins.Count > 0)
        {
            throw new NotSupportedException("Reactive DISTINCT does not support JOINs.");
        }

        if (select.GroupByExpression is not null || select.HavingExpression is not null)
        {
            throw new NotSupportedException("Reactive DISTINCT does not support GROUP BY / HAVING.");
        }

        if (select.OrderByExpression is not null || select.LimitExpression is not null)
        {
            throw new NotSupportedException("Reactive DISTINCT does not support ORDER BY / LIMIT.");
        }

        if (select.Ctes.Count > 0)
        {
            throw new NotSupportedException("Reactive DISTINCT does not support common table expressions.");
        }

        foreach (SelectColumnNode column in select.Columns)
        {
            if (column.Expression is AggregateExpressionNode || column.Expression is WindowFunctionExpressionNode)
            {
                throw new NotSupportedException("Reactive DISTINCT does not support aggregate or window functions.");
            }

            if (column.RawExpression.Contains('(') && !ReactiveTableLayouts.IsStarColumn(column))
            {
                throw new NotSupportedException($"Reactive DISTINCT does not support computed projection '{column.RawExpression}'.");
            }
        }
    }
}
