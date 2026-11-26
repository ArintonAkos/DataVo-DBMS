namespace DataVo.Core.Execution.Volcano;

/// <summary>
/// Binary operator that performs an in-memory hash join over two source streams.
/// </summary>
public sealed class InnerJoinOperator : IQueryOperator
{
    private readonly IQueryOperator _left;
    private readonly IQueryOperator _right;
    private readonly string _leftJoinColumn;
    private readonly string _rightJoinColumn;
    private readonly string _leftTableName;
    private readonly string _rightTableName;

    private readonly Dictionary<string, List<TypedExecutionRow>> _rightLookup = [];
    private TypedExecutionRow? _currentLeft;
    private List<TypedExecutionRow>? _currentMatches;
    private int _currentMatchIndex;
    private long _outputRowId;

    /// <summary>
    /// Initializes an inner join operator over left and right input streams.
    /// </summary>
    public InnerJoinOperator(
        IQueryOperator left,
        IQueryOperator right,
        string leftJoinColumn,
        string rightJoinColumn,
        string leftTableName,
        string rightTableName)
    {
        _left = left;
        _right = right;
        _leftJoinColumn = leftJoinColumn;
        _rightJoinColumn = rightJoinColumn;
        _leftTableName = leftTableName;
        _rightTableName = rightTableName;
    }

    /// <inheritdoc />
    public void Open()
    {
        _outputRowId = 0;
        _currentLeft = null;
        _currentMatches = null;
        _currentMatchIndex = 0;
        _rightLookup.Clear();

        _right.Open();
        try
        {
            while (true)
            {
                var row = _right.GetNextRow();
                if (row == null)
                {
                    break;
                }

                if (!row.Values.TryGetValue(_rightJoinColumn, out var joinValue))
                {
                    continue;
                }

                if (!JoinKeys.TryNormalize(joinValue, out string key))
                {
                    continue; // a NULL key joins with nothing
                }

                if (!_rightLookup.TryGetValue(key, out var bucket))
                {
                    bucket = [];
                    _rightLookup[key] = bucket;
                }

                bucket.Add(row.ToTyped());
            }
        }
        finally
        {
            _right.Close();
        }

        _left.Open();
    }

    /// <inheritdoc />
    public ExecutionRow? GetNextRow()
    {
        while (true)
        {
            if (_currentLeft != null && _currentMatches != null && _currentMatchIndex < _currentMatches.Count)
            {
                var rightRow = _currentMatches[_currentMatchIndex++];
                return MergeRows(_currentLeft, rightRow);
            }

            ExecutionRow? nextLeft = _left.GetNextRow();
            if (nextLeft == null)
            {
                return null;
            }

            _currentLeft = nextLeft.ToTyped();

            _currentMatches = null;
            _currentMatchIndex = 0;

            if (!_currentLeft.Values.TryGetValue(_leftJoinColumn, out var leftJoinValue))
            {
                continue;
            }

            if (JoinKeys.TryNormalize(leftJoinValue, out string key)
                && _rightLookup.TryGetValue(key, out var matches) && matches.Count > 0)
            {
                _currentMatches = matches;
            }
        }
    }

    /// <inheritdoc />
    public void Close()
    {
        _left.Close();
        _currentLeft = null;
        _currentMatches = null;
        _currentMatchIndex = 0;
        _rightLookup.Clear();
    }

    private ExecutionRow MergeRows(TypedExecutionRow leftRow, TypedExecutionRow rightRow)
    {
        int estimatedCapacity = leftRow.Values.Count + rightRow.Values.Count;
        var values = new Dictionary<string, object?>(Math.Max(estimatedCapacity, 1));

        foreach (var cell in leftRow.Values)
        {
            string key = cell.Key.Contains('.') ? cell.Key : $"{_leftTableName}.{cell.Key}";
            values[key] = cell.Value;
        }

        foreach (var cell in rightRow.Values)
        {
            string key = cell.Key.Contains('.') ? cell.Key : $"{_rightTableName}.{cell.Key}";
            values[key] = cell.Value;
        }

        _outputRowId++;
        return new ExecutionRow(_outputRowId, values);
    }
}
