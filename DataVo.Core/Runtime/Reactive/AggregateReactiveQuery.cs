using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Utils;
using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// A single-table per-group incremental aggregate (L2): <c>GROUP BY</c> with
/// <c>COUNT</c>/<c>SUM</c>/<c>AVG</c>/<c>MIN</c>/<c>MAX</c>.
/// </summary>
/// <remarks>
/// State is keyed by group key (the projected <c>GROUP BY</c> columns), so it is independent of the
/// engine's out-of-place <c>UPDATE</c> row-id reassignment. Invertible aggregates
/// (<c>COUNT</c>/<c>SUM</c>/<c>AVG</c>) are maintained by adding/subtracting a row's contribution;
/// non-invertible <c>MIN</c>/<c>MAX</c> are backed by a per-group, per-column value multiset so the
/// next extreme surfaces after the current one is deleted. An optional pre-aggregation <c>WHERE</c>
/// is applied first: a row only contributes to its group while it matches.
/// </remarks>
internal sealed class AggregateReactiveQuery : IReactiveQuery
{
    private enum AggregateFunction
    {
        Count,
        Sum,
        Avg,
        Min,
        Max,
    }

    private sealed record AggregateSpec(string OutputName, AggregateFunction Function, string? Column, bool IsStar);

    private sealed class GroupState
    {
        public long Count;
        public readonly Dictionary<string, long> NonNullCount = new(StringComparer.OrdinalIgnoreCase);
        public readonly Dictionary<string, decimal> Sum = new(StringComparer.OrdinalIgnoreCase);
        public readonly Dictionary<string, bool> SumIsIntegral = new(StringComparer.OrdinalIgnoreCase);
        public readonly Dictionary<string, SortedDictionary<object, long>> Extremes = new(StringComparer.OrdinalIgnoreCase);
    }

    private readonly ReactivePredicate _predicate;
    private readonly List<string> _groupColumns = [];
    private readonly List<AggregateSpec> _aggregates = [];
    private readonly List<(string OutputName, string Column)> _groupOutputs = [];
    private readonly Dictionary<string, GroupState> _groups = new(StringComparer.Ordinal);
    private readonly Dictionary<string, object?[]> _groupKeyValues = new(StringComparer.Ordinal);

    /// <summary>
    /// Compiles the supplied parsed SELECT into an incremental aggregate operator.
    /// </summary>
    /// <param name="select">The parsed aggregate SELECT.</param>
    /// <param name="engine">The owning engine (unused; kept for routing symmetry).</param>
    /// <param name="databaseName">The database that owns the source table (unused; group-keyed state).</param>
    public AggregateReactiveQuery(SelectStatement select, DataVoEngine engine, string databaseName)
    {
        Validate(select);

        Table = select.FromTable!.Name;
        _predicate = ReactivePredicate.Compile(select.WhereExpression);

        if (select.GroupByExpression is not null)
        {
            foreach (IdentifierNode column in select.GroupByExpression.Columns)
            {
                _groupColumns.Add(StripQualifier(column.Name));
            }
        }

        foreach (SelectColumnNode column in select.Columns)
        {
            if (column.Expression is AggregateExpressionNode aggregate)
            {
                _aggregates.Add(BuildAggregateSpec(column, aggregate));
            }
            else
            {
                string source = ResolveGroupColumnName(column);
                string output = column.Alias ?? source;
                _groupOutputs.Add((output, source));
            }
        }
    }

    /// <inheritdoc />
    public string Table { get; }

    /// <inheritdoc />
    public void Seed(IEnumerable<(long RowId, IReadOnlyDictionary<string, object?> Row)> rows)
    {
        foreach ((long _, IReadOnlyDictionary<string, object?> row) in rows)
        {
            if (_predicate.Matches(row))
            {
                AddRow(row);
            }
        }
    }

    /// <inheritdoc />
    public QueryChange Apply(IReadOnlyList<RowChange> tableChanges)
    {
        var touched = new HashSet<string>(StringComparer.Ordinal);

        foreach (RowChange change in tableChanges)
        {
            bool beforeMatches = change.Before is not null && _predicate.Matches(change.Before);
            bool afterMatches = change.After is not null && _predicate.Matches(change.After);

            switch (change.Kind)
            {
                case ChangeKind.Insert:
                    if (afterMatches)
                    {
                        touched.Add(AddRow(change.After!));
                    }

                    break;

                case ChangeKind.Delete:
                    if (beforeMatches)
                    {
                        touched.Add(RemoveRow(change.Before!));
                    }

                    break;

                case ChangeKind.Update:
                    if (beforeMatches)
                    {
                        touched.Add(RemoveRow(change.Before!));
                    }

                    if (afterMatches)
                    {
                        touched.Add(AddRow(change.After!));
                    }

                    break;
            }
        }

        return Classify(touched);
    }

    private QueryChange Classify(HashSet<string> touched)
    {
        List<IReadOnlyDictionary<string, object?>> added = [];
        List<IReadOnlyDictionary<string, object?>> removed = [];
        List<IReadOnlyDictionary<string, object?>> updated = [];

        foreach (string key in touched)
        {
            if (_groups.TryGetValue(key, out GroupState? state))
            {
                IReadOnlyDictionary<string, object?> row = BuildOutputRow(key, state);
                if (_emittedGroups.Add(key))
                {
                    added.Add(row);
                }
                else
                {
                    updated.Add(row);
                }
            }
            else
            {
                if (_emittedGroups.Remove(key))
                {
                    removed.Add(BuildRemovedRow(key));
                }
            }
        }

        return new QueryChange(added, removed, updated);
    }

    private readonly HashSet<string> _emittedGroups = new(StringComparer.Ordinal);

    private string AddRow(IReadOnlyDictionary<string, object?> row)
    {
        string key = ComputeGroupKey(row);
        if (!_groups.TryGetValue(key, out GroupState? state))
        {
            state = new GroupState();
            _groups[key] = state;
            _groupKeyValues[key] = CaptureGroupValues(row);
        }

        state.Count++;

        foreach (AggregateSpec spec in _aggregates)
        {
            if (spec.IsStar || spec.Column is null)
            {
                continue;
            }

            object? value = row.TryGetValue(spec.Column, out object? v) ? v : null;
            if (value is null)
            {
                continue;
            }

            switch (spec.Function)
            {
                case AggregateFunction.Count:
                    state.NonNullCount[spec.Column] = GetOrZero(state.NonNullCount, spec.Column) + 1;
                    break;

                case AggregateFunction.Sum:
                case AggregateFunction.Avg:
                    state.NonNullCount[spec.Column] = GetOrZero(state.NonNullCount, spec.Column) + 1;
                    state.Sum[spec.Column] = GetOrZeroDecimal(state.Sum, spec.Column) + ToDecimal(value);
                    state.SumIsIntegral[spec.Column] =
                        (!state.SumIsIntegral.TryGetValue(spec.Column, out bool integral) || integral) && IsIntegral(value);
                    break;

                case AggregateFunction.Min:
                case AggregateFunction.Max:
                    MultisetAdd(GetExtremes(state, spec.Column), value);
                    break;
            }
        }

        return key;
    }

    private string RemoveRow(IReadOnlyDictionary<string, object?> row)
    {
        string key = ComputeGroupKey(row);
        if (!_groups.TryGetValue(key, out GroupState? state))
        {
            return key;
        }

        state.Count--;

        foreach (AggregateSpec spec in _aggregates)
        {
            if (spec.IsStar || spec.Column is null)
            {
                continue;
            }

            object? value = row.TryGetValue(spec.Column, out object? v) ? v : null;
            if (value is null)
            {
                continue;
            }

            switch (spec.Function)
            {
                case AggregateFunction.Count:
                    state.NonNullCount[spec.Column] = GetOrZero(state.NonNullCount, spec.Column) - 1;
                    break;

                case AggregateFunction.Sum:
                case AggregateFunction.Avg:
                    state.NonNullCount[spec.Column] = GetOrZero(state.NonNullCount, spec.Column) - 1;
                    state.Sum[spec.Column] = GetOrZeroDecimal(state.Sum, spec.Column) - ToDecimal(value);
                    break;

                case AggregateFunction.Min:
                case AggregateFunction.Max:
                    MultisetRemove(GetExtremes(state, spec.Column), value);
                    break;
            }
        }

        if (state.Count <= 0)
        {
            _groups.Remove(key);
        }

        return key;
    }

    private IReadOnlyDictionary<string, object?> BuildOutputRow(string key, GroupState state)
    {
        var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        object?[] groupValues = _groupKeyValues[key];

        for (int i = 0; i < _groupOutputs.Count; i++)
        {
            (string outputName, string sourceColumn) = _groupOutputs[i];
            int idx = _groupColumns.FindIndex(c => c.Equals(sourceColumn, StringComparison.OrdinalIgnoreCase));
            row[outputName] = idx >= 0 ? groupValues[idx] : null;
        }

        foreach (AggregateSpec spec in _aggregates)
        {
            row[spec.OutputName] = ComputeAggregate(spec, state);
        }

        return row;
    }

    private IReadOnlyDictionary<string, object?> BuildRemovedRow(string key)
    {
        var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        object?[] groupValues = _groupKeyValues.TryGetValue(key, out object?[]? values) ? values : [];

        for (int i = 0; i < _groupOutputs.Count; i++)
        {
            (string outputName, string sourceColumn) = _groupOutputs[i];
            int idx = _groupColumns.FindIndex(c => c.Equals(sourceColumn, StringComparison.OrdinalIgnoreCase));
            row[outputName] = idx >= 0 && idx < groupValues.Length ? groupValues[idx] : null;
        }

        // The aggregate columns of a removed group carry no meaningful value; report null.
        foreach (AggregateSpec spec in _aggregates)
        {
            row[spec.OutputName] = null;
        }

        return row;
    }

    private object? ComputeAggregate(AggregateSpec spec, GroupState state)
    {
        switch (spec.Function)
        {
            case AggregateFunction.Count:
                if (spec.IsStar || spec.Column is null)
                {
                    return state.Count;
                }

                return GetOrZero(state.NonNullCount, spec.Column);

            case AggregateFunction.Sum:
            {
                if (spec.Column is null || GetOrZero(state.NonNullCount, spec.Column) == 0)
                {
                    return null;
                }

                decimal sum = GetOrZeroDecimal(state.Sum, spec.Column);
                bool integral = !state.SumIsIntegral.TryGetValue(spec.Column, out bool flag) || flag;
                return integral ? Convert.ToInt64(sum) : sum;
            }

            case AggregateFunction.Avg:
            {
                if (spec.Column is null)
                {
                    return null;
                }

                long n = GetOrZero(state.NonNullCount, spec.Column);
                if (n == 0)
                {
                    return null;
                }

                return GetOrZeroDecimal(state.Sum, spec.Column) / n;
            }

            case AggregateFunction.Min:
            {
                SortedDictionary<object, long> values = GetExtremes(state, spec.Column!);
                return values.Count == 0 ? null : First(values).Key;
            }

            case AggregateFunction.Max:
            {
                SortedDictionary<object, long> values = GetExtremes(state, spec.Column!);
                return values.Count == 0 ? null : Last(values).Key;
            }

            default:
                return null;
        }
    }

    private string ComputeGroupKey(IReadOnlyDictionary<string, object?> row)
    {
        if (_groupColumns.Count == 0)
        {
            return string.Empty;
        }

        return string.Join(
            "",
            _groupColumns.Select(column =>
            {
                object? value = row.TryGetValue(column, out object? v) ? v : null;
                return value is null ? " NULL" : "v:" + Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture);
            }));
    }

    private object?[] CaptureGroupValues(IReadOnlyDictionary<string, object?> row)
    {
        var values = new object?[_groupColumns.Count];
        for (int i = 0; i < _groupColumns.Count; i++)
        {
            values[i] = row.TryGetValue(_groupColumns[i], out object? v) ? v : null;
        }

        return values;
    }

    private static AggregateSpec BuildAggregateSpec(SelectColumnNode column, AggregateExpressionNode aggregate)
    {
        AggregateFunction function = aggregate.FunctionName.ToUpperInvariant() switch
        {
            "COUNT" => AggregateFunction.Count,
            "SUM" => AggregateFunction.Sum,
            "AVG" => AggregateFunction.Avg,
            "MIN" => AggregateFunction.Min,
            "MAX" => AggregateFunction.Max,
            _ => throw new NotSupportedException(
                $"Reactive aggregates do not support the function '{aggregate.FunctionName}'."),
        };

        string? sourceColumn = null;
        if (!aggregate.IsStar && aggregate.Argument is not null)
        {
            sourceColumn = aggregate.Argument switch
            {
                ColumnRefNode columnRef => columnRef.Column,
                ResolvedColumnRefNode resolved => resolved.Column,
                _ => throw new NotSupportedException(
                    "Reactive aggregates support only a bare column argument (for example SUM(Score))."),
            };
        }

        if (function is AggregateFunction.Sum or AggregateFunction.Avg or AggregateFunction.Min or AggregateFunction.Max
            && sourceColumn is null)
        {
            throw new NotSupportedException(
                $"Reactive aggregate '{aggregate.FunctionName}' requires a column argument.");
        }

        string outputName = column.Alias ?? column.RawExpression.Trim();
        return new AggregateSpec(outputName, function, sourceColumn, aggregate.IsStar);
    }

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

    private string ResolveGroupColumnName(SelectColumnNode column)
    {
        if (column.Expression is ColumnRefNode columnRef)
        {
            return columnRef.Column;
        }

        if (column.Expression is ResolvedColumnRefNode resolved)
        {
            return resolved.Column;
        }

        return StripQualifier(column.RawExpression.Trim());
    }

    private static string StripQualifier(string name)
    {
        int dot = name.LastIndexOf('.');
        return dot >= 0 ? name[(dot + 1)..] : name;
    }

    private static SortedDictionary<object, long> GetExtremes(GroupState state, string column)
    {
        if (!state.Extremes.TryGetValue(column, out SortedDictionary<object, long>? values))
        {
            values = new SortedDictionary<object, long>(ReactiveValueComparer.Instance);
            state.Extremes[column] = values;
        }

        return values;
    }

    private static void MultisetAdd(SortedDictionary<object, long> values, object value)
    {
        values[value] = (values.TryGetValue(value, out long count) ? count : 0) + 1;
    }

    private static void MultisetRemove(SortedDictionary<object, long> values, object value)
    {
        if (!values.TryGetValue(value, out long count))
        {
            return;
        }

        if (count <= 1)
        {
            values.Remove(value);
        }
        else
        {
            values[value] = count - 1;
        }
    }

    private static KeyValuePair<object, long> First(SortedDictionary<object, long> values)
    {
        foreach (KeyValuePair<object, long> entry in values)
        {
            return entry;
        }

        return default;
    }

    private static KeyValuePair<object, long> Last(SortedDictionary<object, long> values)
    {
        KeyValuePair<object, long> last = default;
        foreach (KeyValuePair<object, long> entry in values)
        {
            last = entry;
        }

        return last;
    }

    private static long GetOrZero(Dictionary<string, long> map, string key) =>
        map.TryGetValue(key, out long value) ? value : 0;

    private static decimal GetOrZeroDecimal(Dictionary<string, decimal> map, string key) =>
        map.TryGetValue(key, out decimal value) ? value : 0m;

    private static decimal ToDecimal(object value) =>
        Convert.ToDecimal(value, System.Globalization.CultureInfo.InvariantCulture);

    private static bool IsIntegral(object value) =>
        value is byte or sbyte or short or ushort or int or uint or long or ulong;
}
