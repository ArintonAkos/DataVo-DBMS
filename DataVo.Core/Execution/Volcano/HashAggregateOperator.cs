using DataVo.Core.Utils;

namespace DataVo.Core.Execution.Volcano;

/// <summary>
/// Materializing hash aggregate operator over an input stream.
/// </summary>
public sealed class HashAggregateOperator : IQueryOperator
{
    public enum AggregateFunction
    {
        Count,
        Sum,
        Avg,
        Min,
        Max
    }

    public sealed class AggregateSpec
    {
        public AggregateSpec(string outputColumn, AggregateFunction function, Func<ExecutionRow, object?>? argumentSelector = null)
        {
            OutputColumn = outputColumn;
            Function = function;
            ArgumentSelector = argumentSelector;
        }

        public string OutputColumn { get; }
        public AggregateFunction Function { get; }
        public Func<ExecutionRow, object?>? ArgumentSelector { get; }
    }

    private sealed class AggregateState
    {
        public long Count;
        public double Sum;
        public bool HasComparable;
        public object? ComparableValue;
    }

    private readonly IQueryOperator _source;
    private readonly IReadOnlyList<string> _groupKeyColumns;
    private readonly IReadOnlyList<AggregateSpec> _aggregateSpecs;

    private List<ExecutionRow> _rows = [];
    private int _index;

    public HashAggregateOperator(IQueryOperator source, IReadOnlyList<string> groupKeyColumns, IReadOnlyList<AggregateSpec> aggregateSpecs)
    {
        _source = source;
        _groupKeyColumns = groupKeyColumns;
        _aggregateSpecs = aggregateSpecs;
    }

    /// <inheritdoc />
    public void Open()
    {
        _source.Open();

        var groups = new Dictionary<string, GroupAccumulator>(StringComparer.Ordinal);

        try
        {
            while (true)
            {
                ExecutionRow? row = _source.GetNextRow();
                if (row == null)
                {
                    break;
                }

                string groupKey = BuildGroupKey(row, _groupKeyColumns);
                if (!groups.TryGetValue(groupKey, out var accumulator))
                {
                    accumulator = new GroupAccumulator(CloneGroupKeys(row, _groupKeyColumns), _aggregateSpecs.Count);
                    groups[groupKey] = accumulator;
                }

                ApplyRowToAccumulator(row, accumulator);
            }
        }
        finally
        {
            _source.Close();
        }

        _rows = [];
        long rowId = 1;
        foreach (GroupAccumulator accumulator in groups.Values)
        {
            var values = new Dictionary<string, dynamic>(accumulator.GroupKeys, StringComparer.OrdinalIgnoreCase);

            for (int i = 0; i < _aggregateSpecs.Count; i++)
            {
                AggregateSpec spec = _aggregateSpecs[i];
                AggregateState state = accumulator.States[i];
                values[spec.OutputColumn] = FinalizeAggregate(spec, state);
            }

            _rows.Add(new ExecutionRow(rowId++, values));
        }

        _index = 0;
    }

    /// <inheritdoc />
    public ExecutionRow? GetNextRow()
    {
        if (_index >= _rows.Count)
        {
            return null;
        }

        ExecutionRow row = _rows[_index];
        _index++;
        return row;
    }

    /// <inheritdoc />
    public void Close()
    {
        _rows = [];
        _index = 0;
    }

    private void ApplyRowToAccumulator(ExecutionRow row, GroupAccumulator accumulator)
    {
        for (int i = 0; i < _aggregateSpecs.Count; i++)
        {
            AggregateSpec spec = _aggregateSpecs[i];
            AggregateState state = accumulator.States[i];

            if (spec.Function == AggregateFunction.Count)
            {
                if (spec.ArgumentSelector == null)
                {
                    state.Count++;
                }
                else
                {
                    object? value = spec.ArgumentSelector(row);
                    if (value != null)
                    {
                        state.Count++;
                    }
                }

                continue;
            }

            object? argument = spec.ArgumentSelector?.Invoke(row);
            if (argument == null)
            {
                continue;
            }

            switch (spec.Function)
            {
                case AggregateFunction.Sum:
                    state.Sum += Convert.ToDouble(argument);
                    state.Count++;
                    break;
                case AggregateFunction.Avg:
                    state.Sum += Convert.ToDouble(argument);
                    state.Count++;
                    break;
                case AggregateFunction.Min:
                    if (!state.HasComparable || DynamicObjectComparer.Instance.Compare(argument, state.ComparableValue) < 0)
                    {
                        state.ComparableValue = argument;
                        state.HasComparable = true;
                    }

                    break;
                case AggregateFunction.Max:
                    if (!state.HasComparable || DynamicObjectComparer.Instance.Compare(argument, state.ComparableValue) > 0)
                    {
                        state.ComparableValue = argument;
                        state.HasComparable = true;
                    }

                    break;
            }
        }
    }

    private static object? FinalizeAggregate(AggregateSpec spec, AggregateState state)
    {
        return spec.Function switch
        {
            AggregateFunction.Count => state.Count,
            AggregateFunction.Sum => state.Count == 0 ? 0d : state.Sum,
            AggregateFunction.Avg => state.Count == 0 ? 0d : state.Sum / state.Count,
            AggregateFunction.Min => state.ComparableValue,
            AggregateFunction.Max => state.ComparableValue,
            _ => throw new InvalidOperationException($"Unsupported aggregate function '{spec.Function}'.")
        };
    }

    private static Dictionary<string, dynamic> CloneGroupKeys(ExecutionRow row, IReadOnlyList<string> groupKeyColumns)
    {
        var values = new Dictionary<string, dynamic>(StringComparer.OrdinalIgnoreCase);
        foreach (string keyColumn in groupKeyColumns)
        {
            values[keyColumn] = row.Values.TryGetValue(keyColumn, out var value) ? value : null!;
        }

        return values;
    }

    private static string BuildGroupKey(ExecutionRow row, IReadOnlyList<string> groupKeyColumns)
    {
        if (groupKeyColumns.Count == 0)
        {
            return "__global__";
        }

        List<string> parts = [];
        foreach (string keyColumn in groupKeyColumns)
        {
            object? value = row.Values.TryGetValue(keyColumn, out var found) ? found : null;
            string typePart = value?.GetType().Name ?? "<null>";
            string valuePart = value?.ToString() ?? "<null>";
            parts.Add($"{keyColumn}:{typePart}:{valuePart}");
        }

        return string.Join("|", parts);
    }

    private sealed class GroupAccumulator
    {
        public GroupAccumulator(Dictionary<string, dynamic> groupKeys, int aggregateCount)
        {
            GroupKeys = groupKeys;
            States = new AggregateState[aggregateCount];
            for (int i = 0; i < aggregateCount; i++)
            {
                States[i] = new AggregateState();
            }
        }

        public Dictionary<string, dynamic> GroupKeys { get; }
        public AggregateState[] States { get; }
    }
}