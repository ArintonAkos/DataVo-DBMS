using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// A single-table linear (L1) reactive query: a <c>SELECT … WHERE</c> over comparisons,
/// <c>AND</c>/<c>OR</c>, and <c>IS [NOT] NULL</c>.
/// </summary>
/// <remarks>
/// The subscription tracks the set of row IDs currently satisfying its predicate. Given a batch of
/// committed <see cref="RowChange"/>s for its table, it computes the enter/leave/stay transitions
/// (§4.3 of the design spec) against that match-set and returns a <see cref="QueryChange"/>.
/// Comparison and NULL semantics are delegated to the same evaluator the batch single-table WHERE
/// path uses, so reactive results match a full re-execution of the query.
/// </remarks>
public sealed class ReactiveSubscription : IReactiveQuery
{
    private readonly ReactivePredicate _predicate;
    private readonly bool _selectStar;
    private readonly List<string> _projection = [];
    private readonly HashSet<long> _matchSet = [];

    /// <summary>
    /// Parses and validates the supplied SQL as a single-table linear reactive query.
    /// </summary>
    /// <param name="sql">The <c>SELECT … WHERE</c> statement to subscribe to.</param>
    /// <exception cref="NotSupportedException">
    /// Thrown when the query uses a construct not supported by the linear (L1) layer, such as joins,
    /// aggregates, <c>GROUP BY</c>, <c>ORDER BY</c>, <c>LIMIT</c>, <c>DISTINCT</c>, subqueries, or
    /// multiple tables.
    /// </exception>
    public ReactiveSubscription(string sql)
    {
        SelectStatement select = ReactiveQueryParser.ParseSingleSelect(sql);
        Validate(select);

        Table = select.FromTable!.Name;

        foreach (SelectColumnNode column in select.Columns)
        {
            if (IsStarColumn(column))
            {
                _selectStar = true;
                continue;
            }

            string columnName = ResolveProjectedColumnName(column);
            _projection.Add(columnName);
        }

        if (_projection.Count == 0)
        {
            _selectStar = true;
        }

        _predicate = ReactivePredicate.Compile(select.WhereExpression);
    }

    /// <summary>Gets the source table this subscription observes.</summary>
    public string Table { get; }

    /// <inheritdoc />
    public IReadOnlyCollection<string> Tables => [Table];

    /// <summary>
    /// Evaluates the subscription predicate against a single materialized row.
    /// </summary>
    /// <param name="row">The row to test.</param>
    /// <returns><c>true</c> when the row satisfies the predicate (or there is none).</returns>
    public bool Matches(IReadOnlyDictionary<string, object?> row)
    {
        return _predicate.Matches(row);
    }

    /// <summary>
    /// Seeds the match-set from the current table contents without emitting any output.
    /// </summary>
    /// <param name="table">The table whose contents are supplied (always this subscription's table).</param>
    /// <param name="rows">The current <c>(rowId, row)</c> pairs in the table.</param>
    public void Seed(string table, IEnumerable<(long RowId, IReadOnlyDictionary<string, object?> Row)> rows)
    {
        foreach ((long rowId, IReadOnlyDictionary<string, object?> row) in rows)
        {
            if (Matches(row))
            {
                _matchSet.Add(rowId);
            }
        }
    }

    /// <summary>
    /// Applies a batch of committed row changes (already filtered to <see cref="Table"/>) and returns
    /// the resulting added/removed/updated rows.
    /// </summary>
    /// <param name="tableChanges">The committed changes for this subscription's table.</param>
    public QueryChange Apply(IReadOnlyList<RowChange> tableChanges)
    {
        List<IReadOnlyDictionary<string, object?>> added = [];
        List<IReadOnlyDictionary<string, object?>> removed = [];
        List<IReadOnlyDictionary<string, object?>> updated = [];

        foreach (RowChange change in tableChanges)
        {
            // Base the enter/leave/stay decision on the predicate evaluated over the before/after
            // images (the §4.3 matrix). This is robust to the engine's out-of-place UPDATE, which
            // reassigns the physical row id and would otherwise break a row-id-keyed match-set.
            bool beforeMatches = change.Before is not null && Matches(change.Before);
            bool afterMatches = change.After is not null && Matches(change.After);

            switch (change.Kind)
            {
                case ChangeKind.Insert:
                    if (afterMatches)
                    {
                        _matchSet.Add(change.RowId);
                        added.Add(Project(change.After!));
                    }

                    break;

                case ChangeKind.Delete:
                    _matchSet.Remove(change.RowId);
                    if (beforeMatches)
                    {
                        removed.Add(Project(change.Before!));
                    }

                    break;

                case ChangeKind.Update:
                    if (!beforeMatches && afterMatches)
                    {
                        _matchSet.Add(change.RowId);
                        added.Add(Project(change.After!));
                    }
                    else if (beforeMatches && !afterMatches)
                    {
                        _matchSet.Remove(change.RowId);
                        removed.Add(Project(change.Before!));
                    }
                    else if (beforeMatches && afterMatches)
                    {
                        _matchSet.Add(change.RowId);
                        updated.Add(Project(change.After!));
                    }

                    break;
            }
        }

        return new QueryChange(added, removed, updated);
    }

    private IReadOnlyDictionary<string, object?> Project(IReadOnlyDictionary<string, object?> row)
    {
        if (_selectStar)
        {
            return new Dictionary<string, object?>(
                row.ToDictionary(pair => pair.Key, pair => pair.Value),
                StringComparer.OrdinalIgnoreCase);
        }

        var projected = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (string column in _projection)
        {
            projected[column] = row.TryGetValue(column, out object? value) ? value : null;
        }

        return projected;
    }


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

            if (column.RawExpression.Contains('(') && !IsStarColumn(column))
            {
                throw new NotSupportedException($"Reactive subscriptions do not support computed projection '{column.RawExpression}' in the linear (L1) layer.");
            }
        }
    }

    private static bool IsStarColumn(SelectColumnNode column)
    {
        return column.RawExpression.Trim() == "*"
            || (column.Expression is null && column.RawExpression.Trim().EndsWith("*", StringComparison.Ordinal));
    }

    private static string ResolveProjectedColumnName(SelectColumnNode column)
    {
        if (column.Expression is ColumnRefNode columnRef)
        {
            return columnRef.Column;
        }

        if (column.Expression is ResolvedColumnRefNode resolved)
        {
            return resolved.Column;
        }

        string raw = column.RawExpression.Trim();
        int dot = raw.LastIndexOf('.');
        return dot >= 0 ? raw[(dot + 1)..] : raw;
    }
}
