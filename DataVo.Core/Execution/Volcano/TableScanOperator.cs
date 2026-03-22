namespace DataVo.Core.Execution.Volcano;

/// <summary>
/// Source operator that streams rows from an in-memory row collection.
/// </summary>
public sealed class TableScanOperator : IQueryOperator
{
    private readonly IReadOnlyList<ExecutionRow> _rows;
    private int _index;

    /// <summary>
    /// Initializes a table-scan source over a row collection.
    /// </summary>
    public TableScanOperator(IReadOnlyList<ExecutionRow> rows)
    {
        _rows = rows;
        _index = 0;
    }

    /// <inheritdoc />
    public void Open()
    {
        _index = 0;
    }

    /// <inheritdoc />
    public ExecutionRow? GetNextRow()
    {
        if (_index >= _rows.Count)
        {
            return null;
        }

        var row = _rows[_index];
        _index++;
        return row;
    }

    /// <inheritdoc />
    public void Close()
    {
        _index = _rows.Count;
    }
}
