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
internal sealed class AggregateReactiveQuery : IBorrowedReactiveQuery
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
        public readonly Dictionary<string, ReactiveExtremumMultiset> Extremes = new(StringComparer.OrdinalIgnoreCase);
    }

    private readonly ReactivePredicate _predicate;
    private readonly List<string> _groupColumns = [];
    private readonly List<AggregateSpec> _aggregates = [];
    private readonly List<(string OutputName, string Column)> _groupOutputs = [];
    private readonly Dictionary<string, GroupState> _groups = new(StringComparer.Ordinal);
    private readonly Dictionary<string, object?[]> _groupKeyValues = new(StringComparer.Ordinal);

    // Group keys currently present in the delivered output, so a re-touched group is classified as an
    // update rather than a duplicate add, and an emptied group is retracted exactly once.
    private readonly HashSet<string> _emittedGroups = new(StringComparer.Ordinal);

    // Reused per-Apply scratch so the borrowed dispatch hot path does not allocate.
    private readonly HashSet<string> _touched = new(StringComparer.Ordinal);
    private readonly ReactiveRowSchema _outputSchema;
    private readonly QueryChangeBuilder _legacyBuilder;
    private readonly CellValue[] _rowScratch;

    // Maps each group output column to its index within _groupColumns (-1 when not a group column),
    // precomputed so the emit hot path avoids a per-row FindIndex over _groupColumns.
    private readonly int[] _groupOutputValueIndex;

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

        // Output schema = group output columns (declared order) followed by aggregate outputs.
        var outputNames = new string[_groupOutputs.Count + _aggregates.Count];
        for (int i = 0; i < _groupOutputs.Count; i++)
        {
            outputNames[i] = _groupOutputs[i].OutputName;
        }

        for (int j = 0; j < _aggregates.Count; j++)
        {
            outputNames[_groupOutputs.Count + j] = _aggregates[j].OutputName;
        }

        _outputSchema = new ReactiveRowSchema(outputNames);
        _rowScratch = new CellValue[outputNames.Length];
        _legacyBuilder = new QueryChangeBuilder(_outputSchema);

        _groupOutputValueIndex = new int[_groupOutputs.Count];
        for (int i = 0; i < _groupOutputs.Count; i++)
        {
            string sourceColumn = _groupOutputs[i].Column;
            _groupOutputValueIndex[i] = _groupColumns.FindIndex(
                c => c.Equals(sourceColumn, StringComparison.OrdinalIgnoreCase));
        }
    }

    /// <summary>Gets the source table this aggregate observes.</summary>
    public string Table { get; }

    /// <inheritdoc />
    public IReadOnlyCollection<string> Tables => [Table];

    /// <inheritdoc />
    public void Seed(string table, IEnumerable<(long RowId, IReadOnlyDictionary<string, object?> Row)> rows)
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
    public ReactiveRowSchema OutputSchema => _outputSchema;

    /// <summary>Owned path: build the borrowed delta into our own arena, then materialize. Behavior-
    /// identical to the pre-migration <c>Apply</c> (same <see cref="QueryChange"/> shape and values).</summary>
    public QueryChange Apply(IReadOnlyList<RowChange> tableChanges)
    {
        _legacyBuilder.Reset();
        ApplyInto(tableChanges, _legacyBuilder);
        return _legacyBuilder.Build().Materialize();
    }

    /// <inheritdoc />
    public void ApplyInto(IReadOnlyList<RowChange> tableChanges, QueryChangeBuilder builder)
    {
        _touched.Clear();

        foreach (RowChange change in tableChanges)
        {
            bool beforeMatches = change.Before is not null && _predicate.Matches(change.Before);
            bool afterMatches = change.After is not null && _predicate.Matches(change.After);

            switch (change.Kind)
            {
                case ChangeKind.Insert:
                    if (afterMatches)
                    {
                        _touched.Add(AddRow(change.After!));
                    }

                    break;

                case ChangeKind.Delete:
                    if (beforeMatches)
                    {
                        _touched.Add(RemoveRow(change.Before!));
                    }

                    break;

                case ChangeKind.Update:
                    if (beforeMatches)
                    {
                        _touched.Add(RemoveRow(change.Before!));
                    }

                    if (afterMatches)
                    {
                        _touched.Add(AddRow(change.After!));
                    }

                    break;
            }
        }

        ClassifyInto(_touched, builder);
    }

    /// <summary>
    /// Turns the set of groups touched by a batch into borrowed output rows written into
    /// <paramref name="builder"/>: a group still present that was not previously emitted is
    /// <c>Added</c>, one that was emitted is <c>Updated</c>, and a group that disappeared (row count
    /// reached zero) is <c>Removed</c> — the per-group analogue of the DISTINCT
    /// present-iff-count&gt;0 boundary.
    /// </summary>
    private void ClassifyInto(HashSet<string> touched, QueryChangeBuilder builder)
    {
        foreach (string key in touched)
        {
            if (_groups.TryGetValue(key, out GroupState? state))
            {
                WriteGroupRow(key, state);
                if (_emittedGroups.Add(key))
                {
                    builder.AddAddedRow(_rowScratch);
                }
                else
                {
                    builder.AddUpdatedRow(_rowScratch);
                }
            }
            else if (_emittedGroups.Remove(key))
            {
                WriteRemovedRow(key);
                builder.AddRemovedRow(_rowScratch);
            }
        }
    }

    /// <summary>
    /// Writes a present group's output row (group-key values then aggregate results) into the reused
    /// <see cref="_rowScratch"/>, in <see cref="_outputSchema"/> column order.
    /// </summary>
    /// <remarks>
    /// Step 1 (emit-side) reads the boxed group-key values and boxed <see cref="ComputeAggregate"/>
    /// results and wraps them via <c>CellValue.From(object?)</c> — the documented residual boxing,
    /// removed in Step 2 (typed <c>CellValue[]</c> group keys + direct aggregate cell writes).
    /// </remarks>
    private void WriteGroupRow(string key, GroupState state)
    {
        object?[] groupValues = _groupKeyValues[key];
        for (int i = 0; i < _groupOutputs.Count; i++)
        {
            int idx = _groupOutputValueIndex[i];
            _rowScratch[i] = CellValue.From(idx >= 0 ? groupValues[idx] : null);
        }

        for (int j = 0; j < _aggregates.Count; j++)
        {
            _rowScratch[_groupOutputs.Count + j] = CellValue.From(ComputeAggregate(_aggregates[j], state));
        }
    }

    /// <summary>
    /// Writes a removed group's output row into <see cref="_rowScratch"/>: group-key values from the
    /// retained <see cref="_groupKeyValues"/>, with null aggregate cells (a removed group carries no
    /// meaningful aggregate value).
    /// </summary>
    private void WriteRemovedRow(string key)
    {
        object?[] groupValues = _groupKeyValues.TryGetValue(key, out object?[]? values) ? values : [];
        for (int i = 0; i < _groupOutputs.Count; i++)
        {
            int idx = _groupOutputValueIndex[i];
            _rowScratch[i] = CellValue.From(idx >= 0 && idx < groupValues.Length ? groupValues[idx] : null);
        }

        for (int j = 0; j < _aggregates.Count; j++)
        {
            _rowScratch[_groupOutputs.Count + j] = CellValue.Null;
        }
    }

    /// <summary>
    /// Adds a matching row's contribution to its group, creating the group on first sight. Invertible
    /// aggregates accumulate (count, non-null count, running sum); MIN/MAX push the value into the
    /// per-column multiset. Returns the affected group key so the caller can re-classify it.
    /// </summary>
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
                    GetExtremes(state, spec.Column).Add(value);
                    break;
            }
        }

        return key;
    }

    /// <summary>
    /// Subtracts a matching row's contribution from its group (the inverse of <see cref="AddRow"/>):
    /// invertible aggregates decrement, MIN/MAX remove one occurrence from the value multiset so the next
    /// extreme surfaces. A group whose row count falls to zero is dropped. Returns the affected group key.
    /// </summary>
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
                    GetExtremes(state, spec.Column).Remove(value);
                    break;
            }
        }

        if (state.Count <= 0)
        {
            _groups.Remove(key);
        }

        return key;
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
                return GetExtremes(state, spec.Column!).Min;

            case AggregateFunction.Max:
                return GetExtremes(state, spec.Column!).Max;

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

    private static ReactiveExtremumMultiset GetExtremes(GroupState state, string column)
    {
        if (!state.Extremes.TryGetValue(column, out ReactiveExtremumMultiset? values))
        {
            values = new ReactiveExtremumMultiset();
            state.Extremes[column] = values;
        }

        return values;
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
