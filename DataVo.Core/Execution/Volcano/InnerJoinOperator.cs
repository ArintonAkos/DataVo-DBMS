using System.Globalization;

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

                string key = NormalizeJoinKey(joinValue);
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

            string key = NormalizeJoinKey(leftJoinValue);
            if (_rightLookup.TryGetValue(key, out var matches) && matches.Count > 0)
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
        var values = new Dictionary<string, dynamic>();

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

    private static string NormalizeJoinKey(object? value)
    {
        if (value == null)
        {
            return "<NULL>";
        }

        if (value is string text)
        {
            return text.Trim('\'');
        }

        if (value is IConvertible && value is not bool && value is not char)
        {
            try
            {
                decimal numeric = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
                return numeric.ToString(CultureInfo.InvariantCulture);
            }
            catch
            {
            }
        }

        return Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
    }
}
