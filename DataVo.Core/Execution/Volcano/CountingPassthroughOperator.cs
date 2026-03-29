namespace DataVo.Core.Execution.Volcano;

/// <summary>
/// Pass-through operator that counts emitted rows for planner feedback and diagnostics.
/// </summary>
public sealed class CountingPassthroughOperator : IQueryOperator
{
    private readonly IQueryOperator _source;

    /// <summary>
    /// Initializes a counting pass-through over a source operator.
    /// </summary>
    /// <param name="source">The wrapped source operator.</param>
    public CountingPassthroughOperator(IQueryOperator source)
    {
        _source = source;
    }

    /// <summary>
    /// Gets the number of rows emitted during the current open/close cycle.
    /// </summary>
    public long EmittedRows { get; private set; }

    /// <inheritdoc />
    public void Open()
    {
        EmittedRows = 0;
        _source.Open();
    }

    /// <inheritdoc />
    public ExecutionRow? GetNextRow()
    {
        ExecutionRow? row = _source.GetNextRow();
        if (row != null)
        {
            EmittedRows++;
        }

        return row;
    }

    /// <inheritdoc />
    public void Close()
    {
        _source.Close();
    }
}
