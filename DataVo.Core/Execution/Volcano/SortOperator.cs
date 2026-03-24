using System.Text.Json;
using DataVo.Core.Utils;

namespace DataVo.Core.Execution.Volcano;

/// <summary>
/// Unary operator that materializes and sorts its source rows.
/// </summary>
public sealed class SortOperator : IQueryOperator
{
    /// <summary>
    /// Options for enabling external sort spill with run generation and merge.
    /// </summary>
    public sealed class SortExecutionOptions
    {
        /// <summary>
        /// Enables external spill mode when the observed row count exceeds <see cref="SpillThresholdRows"/>.
        /// </summary>
        public bool EnableExternalSpill { get; init; }

        /// <summary>
        /// Row threshold that triggers external run generation.
        /// </summary>
        public int SpillThresholdRows { get; init; } = 50000;

        /// <summary>
        /// Maximum rows per spill run before writing a sorted chunk to disk.
        /// </summary>
        public int SpillRunSizeRows { get; init; } = 5000;

        /// <summary>
        /// Optional directory for spill run files. Defaults to process temporary directory.
        /// </summary>
        public string? SpillDirectory { get; init; }
    }

    /// <summary>
    /// Defines one sort key and its direction.
    /// </summary>
    public sealed class SortKeySpec
    {
        /// <summary>
        /// Initializes a sort key selector and direction.
        /// </summary>
        public SortKeySpec(Func<ExecutionRow, object?> keySelector, bool ascending)
        {
            KeySelector = keySelector;
            Ascending = ascending;
        }

        /// <summary>
        /// Initializes a typed sort key selector and direction.
        /// </summary>
        public SortKeySpec(Func<TypedExecutionRow, object?> typedKeySelector, bool ascending)
        {
            TypedKeySelector = typedKeySelector;
            Ascending = ascending;
        }

        /// <summary>
        /// Key selector used to resolve sort values from a row.
        /// </summary>
        public Func<ExecutionRow, object?> KeySelector { get; }

        /// <summary>
        /// Typed key selector used to resolve sort values from a row.
        /// </summary>
        public Func<TypedExecutionRow, object?>? TypedKeySelector { get; }

        /// <summary>
        /// Sort direction for this key.
        /// </summary>
        public bool Ascending { get; }
    }

    private readonly IQueryOperator _source;
    private readonly IReadOnlyList<SortKeySpec> _sortKeys;
    private readonly SortExecutionOptions _options;

    private List<ExecutionRow> _sortedRows = [];
    private List<string> _spillRunFiles = [];
    private bool _usingExternalSpill;
    private PriorityQueue<RunCursor, SortPriority>? _mergeQueue;
    private int _index;

    /// <summary>
    /// Initializes a sort operator over a source stream.
    /// </summary>
    public SortOperator(IQueryOperator source, Func<ExecutionRow, object?> keySelector, bool ascending)
        : this(source, [new SortKeySpec(keySelector, ascending)], options: null)
    {
    }

    /// <summary>
    /// Initializes a sort operator over a source stream with multiple sort keys.
    /// </summary>
    public SortOperator(IQueryOperator source, IReadOnlyList<SortKeySpec> sortKeys)
        : this(source, sortKeys, options: null)
    {
    }

    /// <summary>
    /// Initializes a sort operator over a source stream with multiple sort keys and spill options.
    /// </summary>
    public SortOperator(IQueryOperator source, IReadOnlyList<SortKeySpec> sortKeys, SortExecutionOptions? options)
    {
        _source = source;
        _sortKeys = sortKeys;
        _options = options ?? new SortExecutionOptions();
    }

    /// <inheritdoc />
    public void Open()
    {
        _usingExternalSpill = false;
        _mergeQueue = null;
        _spillRunFiles = [];

        _source.Open();

        try
        {
            var probeRows = new List<ExecutionRow>();
            while (true)
            {
                var row = _source.GetNextRow();
                if (row == null)
                {
                    break;
                }

                probeRows.Add(row);
                if (ShouldUseExternalSpill(probeRows.Count))
                {
                    BuildSpillRuns(probeRows);
                    _usingExternalSpill = true;
                    break;
                }
            }

            if (!_usingExternalSpill)
            {
                _sortedRows = SortRowsInMemory(probeRows);
            }
            else
            {
                DrainRemainingIntoSpillRuns();
                OpenSpillMergeQueue();
                _sortedRows = [];
            }
        }
        finally
        {
            _source.Close();
        }

        _index = 0;
    }

    /// <inheritdoc />
    public ExecutionRow? GetNextRow()
    {
        if (_usingExternalSpill)
        {
            return GetNextRowFromMergeQueue();
        }

        if (_index >= _sortedRows.Count)
        {
            return null;
        }

        var row = _sortedRows[_index];
        _index++;
        return row;
    }

    /// <inheritdoc />
    public void Close()
    {
        _sortedRows = [];
        DisposeSpillMergeQueue();
        CleanupSpillRuns();
        _spillRunFiles = [];
        _usingExternalSpill = false;
        _index = 0;
    }

    private bool ShouldUseExternalSpill(int observedRows)
    {
        return _options.EnableExternalSpill
            && _options.SpillThresholdRows > 0
            && observedRows > _options.SpillThresholdRows;
    }

    private List<ExecutionRow> SortRowsInMemory(List<ExecutionRow> input)
    {
        if (_sortKeys.Count == 0)
        {
            return input;
        }

        IOrderedEnumerable<ExecutionRow>? ordered = null;

        foreach (SortKeySpec key in _sortKeys)
        {
            if (ordered == null)
            {
                ordered = key.Ascending
                    ? input.OrderBy(row => ResolveSortKeyValue(key, row), DynamicObjectComparer.Instance)
                    : input.OrderByDescending(row => ResolveSortKeyValue(key, row), DynamicObjectComparer.Instance);
            }
            else
            {
                ordered = key.Ascending
                    ? ordered.ThenBy(row => ResolveSortKeyValue(key, row), DynamicObjectComparer.Instance)
                    : ordered.ThenByDescending(row => ResolveSortKeyValue(key, row), DynamicObjectComparer.Instance);
            }
        }

        return ordered?.ToList() ?? input;
    }

    private static object? ResolveSortKeyValue(SortKeySpec key, ExecutionRow row)
    {
        if (key.TypedKeySelector != null)
        {
            return key.TypedKeySelector(row.ToTyped());
        }

        return key.KeySelector(row);
    }

    private void BuildSpillRuns(List<ExecutionRow> initialRows)
    {
        int runSize = Math.Max(1, _options.SpillRunSizeRows);
        List<ExecutionRow> chunk = [];

        foreach (ExecutionRow row in initialRows)
        {
            chunk.Add(row);
            if (chunk.Count >= runSize)
            {
                FlushChunkToRunFile(chunk);
                chunk = [];
            }
        }

        if (chunk.Count > 0)
        {
            FlushChunkToRunFile(chunk);
        }
    }

    private void DrainRemainingIntoSpillRuns()
    {
        int runSize = Math.Max(1, _options.SpillRunSizeRows);
        List<ExecutionRow> chunk = [];

        while (true)
        {
            ExecutionRow? row = _source.GetNextRow();
            if (row == null)
            {
                break;
            }

            chunk.Add(row);
            if (chunk.Count >= runSize)
            {
                FlushChunkToRunFile(chunk);
                chunk = [];
            }
        }

        if (chunk.Count > 0)
        {
            FlushChunkToRunFile(chunk);
        }
    }

    private void FlushChunkToRunFile(List<ExecutionRow> chunk)
    {
        List<ExecutionRow> sortedChunk = SortRowsInMemory(chunk);
        string runFilePath = CreateSpillRunFilePath();
        _spillRunFiles.Add(runFilePath);

        using var writer = new StreamWriter(runFilePath, append: false);
        foreach (ExecutionRow row in sortedChunk)
        {
            TypedExecutionRow typed = row.ToTyped();
            writer.WriteLine(JsonSerializer.Serialize(typed));
        }
    }

    private string CreateSpillRunFilePath()
    {
        string baseDirectory = string.IsNullOrWhiteSpace(_options.SpillDirectory)
            ? Path.GetTempPath()
            : _options.SpillDirectory!;

        Directory.CreateDirectory(baseDirectory);
        return Path.Combine(baseDirectory, $"datavo-sort-run-{Guid.NewGuid():N}.jsonl");
    }

    private void OpenSpillMergeQueue()
    {
        _mergeQueue = new PriorityQueue<RunCursor, SortPriority>(new SortPriorityComparer(_sortKeys));

        foreach (string runFile in _spillRunFiles)
        {
            var cursor = new RunCursor(runFile);
            if (cursor.TryReadNext(out var row))
            {
                _mergeQueue.Enqueue(cursor, BuildPriority(row));
            }
            else
            {
                cursor.Dispose();
            }
        }
    }

    private ExecutionRow? GetNextRowFromMergeQueue()
    {
        if (_mergeQueue == null || _mergeQueue.Count == 0)
        {
            return null;
        }

        _mergeQueue.TryDequeue(out RunCursor? cursor, out SortPriority? priority);
        if (cursor == null || priority == null)
        {
            return null;
        }

        ExecutionRow result = priority.Row;
        if (cursor.TryReadNext(out var nextRow))
        {
            _mergeQueue.Enqueue(cursor, BuildPriority(nextRow));
        }
        else
        {
            cursor.Dispose();
        }

        return result;
    }

    private SortPriority BuildPriority(ExecutionRow row)
    {
        object?[] keys;
        if (_sortKeys.Count == 0)
        {
            keys = [row.RowId];
        }
        else
        {
            keys = new object?[_sortKeys.Count];
            for (int i = 0; i < _sortKeys.Count; i++)
            {
                keys[i] = _sortKeys[i].KeySelector(row);
            }
        }

        return new SortPriority(row, keys);
    }

    private void DisposeSpillMergeQueue()
    {
        if (_mergeQueue == null)
        {
            return;
        }

        while (_mergeQueue.Count > 0)
        {
            _mergeQueue.TryDequeue(out RunCursor? cursor, out _);
            cursor?.Dispose();
        }

        _mergeQueue = null;
    }

    private void CleanupSpillRuns()
    {
        foreach (string file in _spillRunFiles)
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

    private sealed class SortPriority
    {
        public SortPriority(ExecutionRow row, object?[] keys)
        {
            Row = row;
            Keys = keys;
        }

        public ExecutionRow Row { get; }
        public object?[] Keys { get; }
    }

    private sealed class SortPriorityComparer(IReadOnlyList<SortKeySpec> sortKeys) : IComparer<SortPriority>
    {
        private readonly IReadOnlyList<SortKeySpec> _sortKeys = sortKeys;

        public int Compare(SortPriority? x, SortPriority? y)
        {
            if (ReferenceEquals(x, y))
            {
                return 0;
            }

            if (x == null)
            {
                return -1;
            }

            if (y == null)
            {
                return 1;
            }

            int keyCount = Math.Min(x.Keys.Length, y.Keys.Length);
            for (int i = 0; i < keyCount; i++)
            {
                int cmp = DynamicObjectComparer.Instance.Compare(x.Keys[i], y.Keys[i]);
                if (cmp == 0)
                {
                    continue;
                }

                bool asc = i < _sortKeys.Count ? _sortKeys[i].Ascending : true;
                return asc ? cmp : -cmp;
            }

            return x.Row.RowId.CompareTo(y.Row.RowId);
        }
    }

    private sealed class RunCursor : IDisposable
    {
        private readonly StreamReader _reader;

        public RunCursor(string filePath)
        {
            _reader = new StreamReader(filePath);
        }

        public bool TryReadNext(out ExecutionRow row)
        {
            row = null!;
            string? line = _reader.ReadLine();
            if (line == null)
            {
                return false;
            }

            TypedExecutionRow? typed = JsonSerializer.Deserialize<TypedExecutionRow>(line);
            if (typed == null)
            {
                return false;
            }

            var normalized = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (var entry in typed.Values)
            {
                normalized[entry.Key] = NormalizeDeserializedValue(entry.Value);
            }

            row = ExecutionRow.FromTyped(new TypedExecutionRow(typed.RowId, normalized));
            return true;
        }

        public void Dispose()
        {
            _reader.Dispose();
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
}
