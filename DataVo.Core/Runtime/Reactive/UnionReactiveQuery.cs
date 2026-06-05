using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// A two-branch incremental UNION / UNION ALL operator over single-table SELECT branches.
/// </summary>
internal sealed class UnionReactiveQuery : IReactiveQuery
{
    private sealed record BranchSpec(
        string Table,
        ReactivePredicate Predicate,
        IReadOnlyList<string> SourceColumns,
        IReadOnlyList<string> OutputColumns);

    private readonly BranchSpec _left;
    private readonly BranchSpec _right;
    private readonly bool _isAll;
    private readonly Dictionary<string, (IReadOnlyDictionary<string, object?> Row, int Count)> _counts = new(StringComparer.Ordinal);

    public UnionReactiveQuery(UnionSelectStatement union)
    {
        Validate(union);

        _isAll = union.Branches[0].IsAll;
        _left = BuildLeftBranch(union.Left);
        _right = BuildRightBranch(union.Branches[0].Select, _left.OutputColumns);
        Tables = new[] { _left.Table, _right.Table }
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public IReadOnlyCollection<string> Tables { get; }

    public void Seed(string table, IEnumerable<(long RowId, IReadOnlyDictionary<string, object?> Row)> rows)
    {
        foreach (BranchSpec branch in BranchesFor(table))
        {
            foreach ((long _, IReadOnlyDictionary<string, object?> row) in rows)
            {
                if (!branch.Predicate.Matches(row))
                {
                    continue;
                }

                IReadOnlyDictionary<string, object?> projected = Project(branch, row);
                if (!_isAll)
                {
                    AddSeedTuple(projected);
                }
            }
        }
    }

    public QueryChange Apply(IReadOnlyList<RowChange> changes)
    {
        Dictionary<string, (IReadOnlyDictionary<string, object?> Row, int Delta)> deltas = new(StringComparer.Ordinal);

        foreach (RowChange change in changes)
        {
            foreach (BranchSpec branch in BranchesFor(change.Table))
            {
                if (change.Before is not null && change.Kind is ChangeKind.Delete or ChangeKind.Update && branch.Predicate.Matches(change.Before))
                {
                    AddDelta(deltas, Project(branch, change.Before), -1);
                }

                if (change.After is not null && change.Kind is ChangeKind.Insert or ChangeKind.Update && branch.Predicate.Matches(change.After))
                {
                    AddDelta(deltas, Project(branch, change.After), +1);
                }
            }
        }

        return _isAll ? ApplyUnionAll(deltas) : ApplyUnion(deltas);
    }

    private QueryChange ApplyUnionAll(Dictionary<string, (IReadOnlyDictionary<string, object?> Row, int Delta)> deltas)
    {
        List<IReadOnlyDictionary<string, object?>> added = [];
        List<IReadOnlyDictionary<string, object?>> removed = [];

        foreach ((_, (IReadOnlyDictionary<string, object?> row, int delta)) in deltas)
        {
            if (delta > 0)
            {
                for (int i = 0; i < delta; i++)
                {
                    added.Add(row);
                }
            }
            else if (delta < 0)
            {
                for (int i = 0; i < -delta; i++)
                {
                    removed.Add(row);
                }
            }
        }

        return new QueryChange(added, removed, []);
    }

    private QueryChange ApplyUnion(Dictionary<string, (IReadOnlyDictionary<string, object?> Row, int Delta)> deltas)
    {
        List<IReadOnlyDictionary<string, object?>> added = [];
        List<IReadOnlyDictionary<string, object?>> removed = [];

        foreach ((string key, (IReadOnlyDictionary<string, object?> row, int delta)) in deltas)
        {
            _counts.TryGetValue(key, out (IReadOnlyDictionary<string, object?> Row, int Count) state);
            int oldCount = state.Count;
            int newCount = oldCount + delta;

            if (oldCount <= 0 && newCount > 0)
            {
                _counts[key] = (row, newCount);
                added.Add(row);
            }
            else if (oldCount > 0 && newCount <= 0)
            {
                _counts.Remove(key);
                removed.Add(state.Row);
            }
            else if (newCount > 0)
            {
                _counts[key] = (row, newCount);
            }
        }

        return new QueryChange(added, removed, []);
    }

    private IEnumerable<BranchSpec> BranchesFor(string table)
    {
        if (_left.Table.Equals(table, StringComparison.OrdinalIgnoreCase))
        {
            yield return _left;
        }

        if (_right.Table.Equals(table, StringComparison.OrdinalIgnoreCase))
        {
            yield return _right;
        }
    }

    private void AddSeedTuple(IReadOnlyDictionary<string, object?> row)
    {
        string key = DistinctReactiveQuery.TupleKey(row);
        if (_counts.TryGetValue(key, out (IReadOnlyDictionary<string, object?> Row, int Count) state))
        {
            _counts[key] = (state.Row, state.Count + 1);
            return;
        }

        _counts[key] = (row, 1);
    }

    private static void AddDelta(
        Dictionary<string, (IReadOnlyDictionary<string, object?> Row, int Delta)> deltas,
        IReadOnlyDictionary<string, object?> row,
        int weight)
    {
        string key = DistinctReactiveQuery.TupleKey(row);
        if (deltas.TryGetValue(key, out (IReadOnlyDictionary<string, object?> Row, int Delta) existing))
        {
            deltas[key] = (row, existing.Delta + weight);
            return;
        }

        deltas[key] = (row, weight);
    }

    private static IReadOnlyDictionary<string, object?> Project(BranchSpec branch, IReadOnlyDictionary<string, object?> row)
    {
        var projected = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < branch.SourceColumns.Count; i++)
        {
            string source = branch.SourceColumns[i];
            string output = branch.OutputColumns[i];
            projected[output] = row.TryGetValue(source, out object? value) ? value : null;
        }

        return projected;
    }

    private static BranchSpec BuildLeftBranch(SelectStatement select)
    {
        ValidateBranch(select);
        var sources = select.Columns.Select(ResolveProjectedColumnName).ToArray();
        var outputs = select.Columns.Select(ResolveOutputColumnName).ToArray();
        return new BranchSpec(select.FromTable!.Name, ReactivePredicate.Compile(select.WhereExpression), sources, outputs);
    }

    private static BranchSpec BuildRightBranch(SelectStatement select, IReadOnlyList<string> outputColumns)
    {
        ValidateBranch(select);
        var sources = select.Columns.Select(ResolveProjectedColumnName).ToArray();
        if (sources.Length != outputColumns.Count)
        {
            throw new NotSupportedException("Reactive UNION branches must project the same number of columns.");
        }

        return new BranchSpec(select.FromTable!.Name, ReactivePredicate.Compile(select.WhereExpression), sources, outputColumns);
    }

    private static void Validate(UnionSelectStatement union)
    {
        if (union.Branches.Count != 1)
        {
            throw new NotSupportedException("Reactive UNION supports exactly two SELECT branches.");
        }

        if (union.OrderByExpression is not null || union.LimitExpression is not null)
        {
            throw new NotSupportedException("Reactive UNION does not support final ORDER BY / LIMIT.");
        }
    }

    private static void ValidateBranch(SelectStatement select)
    {
        if (select.FromTable is null)
        {
            throw new NotSupportedException("Reactive UNION branches require a single FROM table.");
        }

        if (select.IsDistinct)
        {
            throw new NotSupportedException("Reactive UNION branches do not support DISTINCT.");
        }

        if (select.Joins.Count > 0)
        {
            throw new NotSupportedException("Reactive UNION branches do not support JOINs.");
        }

        if (select.GroupByExpression is not null || select.HavingExpression is not null)
        {
            throw new NotSupportedException("Reactive UNION branches do not support GROUP BY / HAVING.");
        }

        if (select.OrderByExpression is not null || select.LimitExpression is not null)
        {
            throw new NotSupportedException("Reactive UNION branches do not support ORDER BY / LIMIT.");
        }

        if (select.Ctes.Count > 0)
        {
            throw new NotSupportedException("Reactive UNION branches do not support common table expressions.");
        }

        foreach (SelectColumnNode column in select.Columns)
        {
            if (IsStarColumn(column))
            {
                throw new NotSupportedException("Reactive UNION branches do not support wildcard projection.");
            }

            if (column.Expression is AggregateExpressionNode || column.Expression is WindowFunctionExpressionNode)
            {
                throw new NotSupportedException("Reactive UNION branches do not support aggregate or window functions.");
            }

            if (column.RawExpression.Contains('('))
            {
                throw new NotSupportedException($"Reactive UNION branches do not support computed projection '{column.RawExpression}'.");
            }
        }
    }

    private static bool IsStarColumn(SelectColumnNode column)
    {
        return column.RawExpression.Trim() == "*"
            || (column.Expression is null && column.RawExpression.Trim().EndsWith("*", StringComparison.Ordinal));
    }

    private static string ResolveOutputColumnName(SelectColumnNode column) =>
        column.Alias ?? ResolveProjectedColumnName(column);

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
