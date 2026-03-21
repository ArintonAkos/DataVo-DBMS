using DataVo.Core.Utils;

namespace DataVo.Core.Execution.Volcano;

/// <summary>
/// Unary operator that materializes and sorts its source rows.
/// </summary>
public sealed class SortOperator : IQueryOperator
{
    private readonly IQueryOperator _source;
    private readonly Func<ExecutionRow, object?> _keySelector;
    private readonly bool _ascending;

    private List<ExecutionRow> _sortedRows = [];
    private int _index;

    /// <summary>
    /// Initializes a sort operator over a source stream.
    /// </summary>
    public SortOperator(IQueryOperator source, Func<ExecutionRow, object?> keySelector, bool ascending)
    {
        _source = source;
        _keySelector = keySelector;
        _ascending = ascending;
    }

    /// <inheritdoc />
    public void Open()
    {
        _source.Open();

        try
        {
            _sortedRows = [];
            while (true)
            {
                var row = _source.GetNextRow();
                if (row == null)
                {
                    break;
                }

                _sortedRows.Add(row);
            }
        }
        finally
        {
            _source.Close();
        }

        _sortedRows = _ascending
            ? [.. _sortedRows.OrderBy(_keySelector, DynamicObjectComparer.Instance)]
            : [.. _sortedRows.OrderByDescending(_keySelector, DynamicObjectComparer.Instance)];

        _index = 0;
    }

    /// <inheritdoc />
    public ExecutionRow? GetNextRow()
    {
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
        _index = 0;
    }
}
