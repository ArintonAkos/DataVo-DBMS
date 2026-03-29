using System.Globalization;

namespace DataVo.Core.Execution.Volcano;

/// <summary>
/// Binary operator that performs an in-memory nested-loop join over two source streams.
/// </summary>
public sealed class NestedLoopJoinOperator : IQueryOperator
{
    private readonly IQueryOperator _left;
    private readonly IQueryOperator _right;
    private readonly string _leftJoinColumn;
    private readonly string _rightJoinColumn;
    private readonly string _leftTableName;
    private readonly string _rightTableName;

    private readonly List<TypedExecutionRow> _rightRows = [];
    private TypedExecutionRow? _currentLeft;
    private int _rightIndex;
    private long _outputRowId;

    public NestedLoopJoinOperator(
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

    public void Open()
    {
        _outputRowId = 0;
        _currentLeft = null;
        _rightIndex = 0;
        _rightRows.Clear();

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

                _rightRows.Add(row.ToTyped());
            }
        }
        finally
        {
            _right.Close();
        }

        _left.Open();
    }

    public ExecutionRow? GetNextRow()
    {
        while (true)
        {
            if (_currentLeft == null)
            {
                ExecutionRow? leftRow = _left.GetNextRow();
                _currentLeft = leftRow?.ToTyped();
                _rightIndex = 0;

                if (_currentLeft == null)
                {
                    return null;
                }
            }

            if (!_currentLeft.Values.TryGetValue(_leftJoinColumn, out var leftJoinValue))
            {
                _currentLeft = null;
                continue;
            }

            string leftKey = NormalizeJoinKey(leftJoinValue);
            while (_rightIndex < _rightRows.Count)
            {
                var rightRow = _rightRows[_rightIndex++];
                if (!rightRow.Values.TryGetValue(_rightJoinColumn, out var rightJoinValue))
                {
                    continue;
                }

                if (leftKey.Equals(NormalizeJoinKey(rightJoinValue), StringComparison.Ordinal))
                {
                    return MergeRows(_currentLeft, rightRow);
                }
            }

            _currentLeft = null;
        }
    }

    public void Close()
    {
        _left.Close();
        _currentLeft = null;
        _rightIndex = 0;
        _rightRows.Clear();
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
