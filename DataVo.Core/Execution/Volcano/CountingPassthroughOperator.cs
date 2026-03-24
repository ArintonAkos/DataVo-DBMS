namespace DataVo.Core.Execution.Volcano;

/// <summary>
/// Pass-through operator that counts emitted rows for planner feedback and diagnostics.
/// </summary>
public sealed class CountingPassthroughOperator : IQueryOperator
{
    private readonly IQueryOperator _source;

    public CountingPassthroughOperator(IQueryOperator source)
    {
        _source = source;
    }

    /// <summary>
    /// Gets the number of rows emitted during the current open/close cycle.
    /// </summary>
    public long EmittedRows { get; private set; }

    public void Open()
    {
        EmittedRows = 0;
        _source.Open();
    }

    public ExecutionRow? GetNextRow()
    {
        ExecutionRow? row = _source.GetNextRow();
        if (row != null)
        {
            EmittedRows++;
        }

        return row;
    }

    public void Close()
    {
        _source.Close();
    }
}
