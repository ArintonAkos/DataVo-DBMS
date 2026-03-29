namespace DataVo.Core.Execution.Volcano;

/// <summary>
/// Unary operator that projects each row to a selected shape.
/// </summary>
public sealed class ProjectOperator : IQueryOperator
{
    private readonly IQueryOperator _source;
    private readonly Func<ExecutionRow, Dictionary<string, object?>>? _projector;
    private readonly Func<TypedExecutionRow, Dictionary<string, object?>>? _typedProjector;

    /// <summary>
    /// Initializes a projection operator over a source stream.
    /// </summary>
    public ProjectOperator(IQueryOperator source, Func<ExecutionRow, Dictionary<string, object?>> projector)
    {
        _source = source;
        _projector = projector;
    }

    /// <summary>
    /// Initializes a projection operator over a source stream using typed row payloads.
    /// </summary>
    public ProjectOperator(IQueryOperator source, Func<TypedExecutionRow, Dictionary<string, object?>> typedProjector)
    {
        _source = source;
        _typedProjector = typedProjector;
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

        if (_typedProjector != null)
        {
            TypedExecutionRow typed = row.ToTyped();
            var projected = _typedProjector(typed);
            return ExecutionRow.FromTyped(new TypedExecutionRow(row.RowId, projected));
        }

        return new ExecutionRow(row.RowId, _projector!(row));
    }

    /// <inheritdoc />
    public void Close()
    {
        _source.Close();
    }
}
