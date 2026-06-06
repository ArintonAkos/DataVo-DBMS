using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// A two-branch incremental UNION / UNION ALL operator over single-table SELECT branches.
/// </summary>
internal sealed class UnionReactiveQuery : IReactiveQuery
{
    private sealed record SeedBranch(
        string Table,
        ReactivePredicate Predicate,
        IReadOnlyList<string> SourceColumns,
        IReadOnlyList<string> OutputColumns);

    private sealed record BranchAdapter(
        IReactiveQuery Query,
        IReadOnlyList<string> ChildOutputColumns,
        IReadOnlyList<string> UnionOutputColumns);

    private readonly SeedBranch _leftSeed;
    private readonly SeedBranch _rightSeed;
    private readonly BranchAdapter _left;
    private readonly BranchAdapter _right;
    private readonly bool _isAll;
    private readonly Dictionary<string, (IReadOnlyDictionary<string, object?> Row, int Count)> _counts = new(StringComparer.Ordinal);

    public UnionReactiveQuery(UnionSelectStatement union, Func<SelectStatement, IReactiveQuery> childFactory)
    {
        Validate(union);

        _isAll = union.Branches[0].IsAll;
        _leftSeed = BuildLeftSeedBranch(union.Left);
        _rightSeed = BuildRightSeedBranch(union.Branches[0].Select, _leftSeed.OutputColumns);
        _left = BuildAdapter(union.Left, _leftSeed.OutputColumns, childFactory);
        _right = BuildAdapter(union.Branches[0].Select, _leftSeed.OutputColumns, childFactory);

        Tables = _left.Query.Tables
            .Concat(_right.Query.Tables)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public IReadOnlyCollection<string> Tables { get; }

    public void Seed(string table, IEnumerable<(long RowId, IReadOnlyDictionary<string, object?> Row)> rows)
    {
        var materialized = rows.ToArray();

        foreach (BranchAdapter branch in BranchesFor(table))
        {
            branch.Query.Seed(table, materialized);
        }

        if (_isAll)
        {
            return;
        }

        foreach (SeedBranch branch in SeedBranchesFor(table))
        {
            foreach ((long _, IReadOnlyDictionary<string, object?> row) in materialized)
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

        foreach (BranchAdapter branch in new[] { _left, _right })
        {
            List<RowChange> relevant = changes
                .Where(change => branch.Query.Tables.Any(table => table.Equals(change.Table, StringComparison.OrdinalIgnoreCase)))
                .ToList();

            if (relevant.Count == 0)
            {
                continue;
            }

            QueryChange childChange = branch.Query.Apply(relevant);
            AddChildDeltas(deltas, branch, childChange);
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

    private IEnumerable<BranchAdapter> BranchesFor(string table)
    {
        if (_left.Query.Tables.Any(observed => observed.Equals(table, StringComparison.OrdinalIgnoreCase)))
        {
            yield return _left;
        }

        if (_right.Query.Tables.Any(observed => observed.Equals(table, StringComparison.OrdinalIgnoreCase)))
        {
            yield return _right;
        }
    }

    private IEnumerable<SeedBranch> SeedBranchesFor(string table)
    {
        if (_leftSeed.Table.Equals(table, StringComparison.OrdinalIgnoreCase))
        {
            yield return _leftSeed;
        }

        if (_rightSeed.Table.Equals(table, StringComparison.OrdinalIgnoreCase))
        {
            yield return _rightSeed;
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

    private static void AddChildDeltas(
        Dictionary<string, (IReadOnlyDictionary<string, object?> Row, int Delta)> deltas,
        BranchAdapter branch,
        QueryChange childChange)
    {
        foreach (IReadOnlyDictionary<string, object?> row in childChange.Added)
        {
            AddDelta(deltas, Normalize(branch, row), +1);
        }

        foreach (IReadOnlyDictionary<string, object?> row in childChange.Removed)
        {
            AddDelta(deltas, Normalize(branch, row), -1);
        }

        if (childChange.Updated.Count > 0 && childChange.UpdatedBefore.Count != childChange.Updated.Count)
        {
            throw new NotSupportedException("Reactive UNION child updates require before-images.");
        }

        for (int i = 0; i < childChange.Updated.Count; i++)
        {
            AddDelta(deltas, Normalize(branch, childChange.UpdatedBefore[i]), -1);
            AddDelta(deltas, Normalize(branch, childChange.Updated[i]), +1);
        }
    }

    private static IReadOnlyDictionary<string, object?> Normalize(
        BranchAdapter branch,
        IReadOnlyDictionary<string, object?> row)
    {
        var normalized = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < branch.ChildOutputColumns.Count; i++)
        {
            string child = branch.ChildOutputColumns[i];
            string output = branch.UnionOutputColumns[i];
            normalized[output] = row.TryGetValue(child, out object? value) ? value : null;
        }

        return normalized;
    }

    private static IReadOnlyDictionary<string, object?> Project(SeedBranch branch, IReadOnlyDictionary<string, object?> row)
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

    private static SeedBranch BuildLeftSeedBranch(SelectStatement select)
    {
        ValidateBranch(select);
        var sources = select.Columns.Select(ResolveProjectedColumnName).ToArray();
        var outputs = select.Columns.Select(ResolveOutputColumnName).ToArray();
        return new SeedBranch(select.FromTable!.Name, ReactivePredicate.Compile(select.WhereExpression), sources, outputs);
    }

    private static SeedBranch BuildRightSeedBranch(SelectStatement select, IReadOnlyList<string> outputColumns)
    {
        ValidateBranch(select);
        var sources = select.Columns.Select(ResolveProjectedColumnName).ToArray();
        if (sources.Length != outputColumns.Count)
        {
            throw new NotSupportedException("Reactive UNION branches must project the same number of columns.");
        }

        return new SeedBranch(select.FromTable!.Name, ReactivePredicate.Compile(select.WhereExpression), sources, outputColumns);
    }

    private static BranchAdapter BuildAdapter(
        SelectStatement select,
        IReadOnlyList<string> unionOutputColumns,
        Func<SelectStatement, IReactiveQuery> childFactory)
    {
        var childOutputColumns = select.Columns.Select(ResolveOutputColumnName).ToArray();
        if (childOutputColumns.Length != unionOutputColumns.Count)
        {
            throw new NotSupportedException("Reactive UNION branches must project the same number of columns.");
        }

        return new BranchAdapter(childFactory(select), childOutputColumns, unionOutputColumns);
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
