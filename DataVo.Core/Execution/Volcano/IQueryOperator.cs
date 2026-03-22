namespace DataVo.Core.Execution.Volcano;

/// <summary>
/// Volcano-style query operator contract.
/// </summary>
public interface IQueryOperator
{
    /// <summary>
    /// Resets the operator to the start of its stream.
    /// </summary>
    void Open();

    /// <summary>
    /// Returns the next row from the operator pipeline, or null when exhausted.
    /// </summary>
    ExecutionRow? GetNextRow();

    /// <summary>
    /// Releases resources owned by the operator.
    /// </summary>
    void Close();
}
