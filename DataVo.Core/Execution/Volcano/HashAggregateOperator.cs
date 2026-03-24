using System.Text.Json;
using DataVo.Core.Utils;

namespace DataVo.Core.Execution.Volcano;

/// <summary>
/// Materializing hash aggregate operator over an input stream.
/// </summary>
public sealed class HashAggregateOperator : IQueryOperator
{
    /// <summary>
    /// Options for enabling external spill with partitioning and merge/reduce aggregation.
    /// </summary>
    public sealed class AggregateExecutionOptions
    {
        /// <summary>
        /// Enables external partition spill when source row count exceeds <see cref="SpillThresholdRows"/>.
        /// </summary>
        public bool EnableExternalSpill { get; init; }

        /// <summary>
        /// Row threshold that triggers external partition spill.
        /// </summary>
        public int SpillThresholdRows { get; init; } = 50000;

        /// <summary>
        /// Number of hash partitions used during spill run generation.
        /// </summary>
        public int PartitionCount { get; init; } = 16;

        /// <summary>
        /// Optional directory for partition run files. Defaults to process temporary directory.
        /// </summary>
        public string? SpillDirectory { get; init; }

        /// <summary>
        /// Enables adaptive partition sizing based on observed spill row volume.
        /// </summary>
        public bool EnableAdaptivePartitioning { get; init; } = true;

        /// <summary>
        /// Target rows per partition when adaptive sizing is enabled.
        /// </summary>
        public int TargetRowsPerPartition { get; init; } = 4096;

        /// <summary>
        /// Maximum partition count for adaptive partition sizing.
        /// </summary>
        public int MaxPartitionCount { get; init; } = 128;
    }

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

        public AggregateSpec(string outputColumn, AggregateFunction function, Func<TypedExecutionRow, object?> typedArgumentSelector)
        {
            OutputColumn = outputColumn;
            Function = function;
            TypedArgumentSelector = typedArgumentSelector;
        }

        public string OutputColumn { get; }
        public AggregateFunction Function { get; }
        public Func<ExecutionRow, object?>? ArgumentSelector { get; }
        public Func<TypedExecutionRow, object?>? TypedArgumentSelector { get; }
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
    private readonly AggregateExecutionOptions _options;

    private List<ExecutionRow> _rows = [];
    private int _index;

    public HashAggregateOperator(IQueryOperator source, IReadOnlyList<string> groupKeyColumns, IReadOnlyList<AggregateSpec> aggregateSpecs)
        : this(source, groupKeyColumns, aggregateSpecs, options: null)
    {
    }

    /// <summary>
    /// Initializes a hash aggregate operator over a source stream with spill options.
    /// </summary>
    public HashAggregateOperator(
        IQueryOperator source,
        IReadOnlyList<string> groupKeyColumns,
        IReadOnlyList<AggregateSpec> aggregateSpecs,
        AggregateExecutionOptions? options)
    {
        _source = source;
        _groupKeyColumns = groupKeyColumns;
        _aggregateSpecs = aggregateSpecs;
        _options = options ?? new AggregateExecutionOptions();
    }

    /// <inheritdoc />
    public void Open()
    {
        var groups = ExecuteAggregatePlan();

        _rows = [];
        long rowId = 1;
        foreach (GroupAccumulator accumulator in groups.Values)
        {
            var values = new Dictionary<string, dynamic>(StringComparer.OrdinalIgnoreCase);
            foreach (var key in accumulator.GroupKeys)
            {
                values[key.Key] = key.Value;
            }

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

    private Dictionary<string, GroupAccumulator> ExecuteAggregatePlan()
    {
        _source.Open();
        try
        {
            var observedRows = new List<ExecutionRow>();
            int observedCount = 0;

            while (true)
            {
                ExecutionRow? row = _source.GetNextRow();
                if (row == null)
                {
                    break;
                }

                observedRows.Add(row);
                observedCount++;

                if (ShouldUseExternalSpill(observedCount))
                {
                    return ExecuteWithExternalSpill(observedRows);
                }
            }

            return ExecuteInMemory(observedRows);
        }
        finally
        {
            _source.Close();
        }
    }

    private bool ShouldUseExternalSpill(int observedRows)
    {
        return _options.EnableExternalSpill
            && _options.SpillThresholdRows > 0
            && observedRows > _options.SpillThresholdRows;
    }

    private Dictionary<string, GroupAccumulator> ExecuteInMemory(List<ExecutionRow> rows)
    {
        var groups = new Dictionary<string, GroupAccumulator>(StringComparer.Ordinal);
        foreach (ExecutionRow row in rows)
        {
            UpsertAggregate(groups, row);
        }

        return groups;
    }

    private Dictionary<string, GroupAccumulator> ExecuteWithExternalSpill(List<ExecutionRow> initialRows)
    {
        int partitionCount = ResolvePartitionCount(initialRows.Count);
        List<string> partitionFiles = CreatePartitionFiles(partitionCount);
        List<StreamWriter> writers = partitionFiles.Select(path => new StreamWriter(path, append: false)).ToList();

        try
        {
            foreach (ExecutionRow row in initialRows)
            {
                WriteRowToPartition(row.ToTyped(), partitionFiles, writers);
            }

            while (true)
            {
                ExecutionRow? row = _source.GetNextRow();
                if (row == null)
                {
                    break;
                }

                WriteRowToPartition(row.ToTyped(), partitionFiles, writers);
            }
        }
        finally
        {
            foreach (var writer in writers)
            {
                writer.Dispose();
            }
        }

        var global = new Dictionary<string, GroupAccumulator>(StringComparer.Ordinal);
        try
        {
            foreach (string file in partitionFiles)
            {
                Dictionary<string, GroupAccumulator> partial = ReducePartition(file);
                MergePartial(global, partial);
            }
        }
        finally
        {
            CleanupPartitionFiles(partitionFiles);
        }

        return global;
    }

    private int ResolvePartitionCount(int observedRows)
    {
        int minPartitionCount = Math.Max(2, _options.PartitionCount);
        if (!_options.EnableAdaptivePartitioning)
        {
            return minPartitionCount;
        }

        int targetRows = Math.Max(1, _options.TargetRowsPerPartition);
        int adaptive = (int)Math.Ceiling((double)Math.Max(1, observedRows) / targetRows);
        int cappedAdaptive = Math.Clamp(adaptive, minPartitionCount, Math.Max(minPartitionCount, _options.MaxPartitionCount));
        return Math.Max(minPartitionCount, cappedAdaptive);
    }

    private List<string> CreatePartitionFiles(int partitionCount)
    {
        string baseDirectory = string.IsNullOrWhiteSpace(_options.SpillDirectory)
            ? Path.GetTempPath()
            : _options.SpillDirectory!;

        Directory.CreateDirectory(baseDirectory);

        List<string> files = [];
        for (int i = 0; i < partitionCount; i++)
        {
            files.Add(Path.Combine(baseDirectory, $"datavo-agg-part-{Guid.NewGuid():N}-{i}.jsonl"));
        }

        return files;
    }

    private void WriteRowToPartition(TypedExecutionRow typed, List<string> partitionFiles, List<StreamWriter> writers)
    {
        string key = BuildGroupKey(typed, _groupKeyColumns);
        int index = (key.GetHashCode() & int.MaxValue) % partitionFiles.Count;
        writers[index].WriteLine(JsonSerializer.Serialize(typed));
    }

    private Dictionary<string, GroupAccumulator> ReducePartition(string partitionFile)
    {
        var groups = new Dictionary<string, GroupAccumulator>(StringComparer.Ordinal);
        using var reader = new StreamReader(partitionFile);

        while (true)
        {
            string? line = reader.ReadLine();
            if (line == null)
            {
                break;
            }

            TypedExecutionRow? typed = JsonSerializer.Deserialize<TypedExecutionRow>(line);
            if (typed == null)
            {
                continue;
            }

            var normalized = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (var entry in typed.Values)
            {
                normalized[entry.Key] = NormalizeDeserializedValue(entry.Value);
            }

            UpsertAggregate(groups, ExecutionRow.FromTyped(new TypedExecutionRow(typed.RowId, normalized)));
        }

        return groups;
    }

    private void MergePartial(
        Dictionary<string, GroupAccumulator> global,
        Dictionary<string, GroupAccumulator> partial)
    {
        foreach (var entry in partial)
        {
            if (!global.TryGetValue(entry.Key, out var accumulator))
            {
                global[entry.Key] = entry.Value;
                continue;
            }

            MergeAccumulatorStates(accumulator, entry.Value);
        }
    }

    private void MergeAccumulatorStates(GroupAccumulator target, GroupAccumulator source)
    {
        for (int i = 0; i < _aggregateSpecs.Count; i++)
        {
            AggregateSpec spec = _aggregateSpecs[i];
            AggregateState targetState = target.States[i];
            AggregateState sourceState = source.States[i];

            switch (spec.Function)
            {
                case AggregateFunction.Count:
                    targetState.Count += sourceState.Count;
                    break;
                case AggregateFunction.Sum:
                case AggregateFunction.Avg:
                    targetState.Sum += sourceState.Sum;
                    targetState.Count += sourceState.Count;
                    break;
                case AggregateFunction.Min:
                    if (sourceState.HasComparable
                        && (!targetState.HasComparable || DynamicObjectComparer.Instance.Compare(sourceState.ComparableValue, targetState.ComparableValue) < 0))
                    {
                        targetState.ComparableValue = sourceState.ComparableValue;
                        targetState.HasComparable = true;
                    }

                    break;
                case AggregateFunction.Max:
                    if (sourceState.HasComparable
                        && (!targetState.HasComparable || DynamicObjectComparer.Instance.Compare(sourceState.ComparableValue, targetState.ComparableValue) > 0))
                    {
                        targetState.ComparableValue = sourceState.ComparableValue;
                        targetState.HasComparable = true;
                    }

                    break;
            }
        }
    }

    private static void CleanupPartitionFiles(IEnumerable<string> files)
    {
        foreach (string file in files)
        {
            try
            {
                if (File.Exists(file))
                {
                    File.Delete(file);
                }
            }
            catch
            {
            }
        }
    }

    private static object? NormalizeDeserializedValue(object? value)
    {
        if (value is not JsonElement element)
        {
            return value;
        }

        return element.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.String => element.GetString(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Number => element.TryGetInt64(out long int64)
                ? int64
                : element.GetDouble(),
            _ => element.ToString()
        };
    }

    private void UpsertAggregate(Dictionary<string, GroupAccumulator> groups, ExecutionRow row)
    {
        TypedExecutionRow typedRow = row.ToTyped();
        string groupKey = BuildGroupKey(typedRow, _groupKeyColumns);
        if (!groups.TryGetValue(groupKey, out var accumulator))
        {
            accumulator = new GroupAccumulator(CloneGroupKeys(typedRow, _groupKeyColumns), _aggregateSpecs.Count);
            groups[groupKey] = accumulator;
        }

        ApplyRowToAccumulator(typedRow, accumulator);
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

    private void ApplyRowToAccumulator(TypedExecutionRow row, GroupAccumulator accumulator)
    {
        ExecutionRow? dynamicRow = null;

        for (int i = 0; i < _aggregateSpecs.Count; i++)
        {
            AggregateSpec spec = _aggregateSpecs[i];
            AggregateState state = accumulator.States[i];

            if (spec.Function == AggregateFunction.Count)
            {
                if (spec.ArgumentSelector == null && spec.TypedArgumentSelector == null)
                {
                    state.Count++;
                }
                else
                {
                    object? value = ResolveAggregateArgumentValue(spec, row, ref dynamicRow);
                    if (value != null)
                    {
                        state.Count++;
                    }
                }

                continue;
            }

            object? argument = ResolveAggregateArgumentValue(spec, row, ref dynamicRow);
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

    private static object? ResolveAggregateArgumentValue(
        AggregateSpec spec,
        TypedExecutionRow row,
        ref ExecutionRow? dynamicRow)
    {
        if (spec.TypedArgumentSelector != null)
        {
            return spec.TypedArgumentSelector(row);
        }

        if (spec.ArgumentSelector == null)
        {
            return null;
        }

        dynamicRow ??= ExecutionRow.FromTyped(row);
        return spec.ArgumentSelector(dynamicRow);
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

    private static Dictionary<string, object?> CloneGroupKeys(TypedExecutionRow row, IReadOnlyList<string> groupKeyColumns)
    {
        var values = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (string keyColumn in groupKeyColumns)
        {
            values[keyColumn] = row.Values.TryGetValue(keyColumn, out var value) ? value : null!;
        }

        return values;
    }

    private static string BuildGroupKey(TypedExecutionRow row, IReadOnlyList<string> groupKeyColumns)
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
        public GroupAccumulator(Dictionary<string, object?> groupKeys, int aggregateCount)
        {
            GroupKeys = groupKeys;
            States = new AggregateState[aggregateCount];
            for (int i = 0; i < aggregateCount; i++)
            {
                States[i] = new AggregateState();
            }
        }

        public Dictionary<string, object?> GroupKeys { get; }
        public AggregateState[] States { get; }
    }
}