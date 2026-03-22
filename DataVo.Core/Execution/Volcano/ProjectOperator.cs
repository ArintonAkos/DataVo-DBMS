namespace DataVo.Core.Execution.Volcano;

/// <summary>
/// Unary operator that projects each row to a selected shape.
/// </summary>
public sealed class ProjectOperator : IQueryOperator
{
    private readonly IQueryOperator _source;
    private readonly Func<ExecutionRow, Dictionary<string, dynamic>> _projector;

    /// <summary>
    /// Initializes a projection operator over a source stream.
    /// </summary>
    public ProjectOperator(IQueryOperator source, Func<ExecutionRow, Dictionary<string, dynamic>> projector)
    {
        _source = source;
        _projector = projector;
    }

    /// <inheritdoc />
    public void Open()
    {
        _source.Open();
    }

    /// <inheritdoc />
    public ExecutionRow? GetNextRow()
    {
        var row = _source.GetNextRow();
        if (row == null)
        {
            return null;
        }

        return new ExecutionRow(row.RowId, _projector(row));
    }

    /// <inheritdoc />
    public void Close()
    {
        _source.Close();
    }
}
