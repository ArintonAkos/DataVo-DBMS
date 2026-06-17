namespace DataVo.Core.Execution.Volcano;

/// <summary>
/// Helper for executing Volcano operator pipelines.
/// </summary>
public static class OperatorPipelineRunner
{
    /// <summary>
    /// Executes a pipeline from its root operator and materializes all produced rows.
    /// </summary>
    public static List<ExecutionRow> ExecuteToList(IQueryOperator root)
        => ExecuteToList(root, maxRows: null);

    /// <summary>
    /// Executes a pipeline from its root operator and materializes up to <paramref name="maxRows"/> rows.
    /// </summary>
    public static List<ExecutionRow> ExecuteToList(IQueryOperator root, int? maxRows)
    {
        if (maxRows.HasValue && maxRows.Value < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxRows));
        }

        var result = maxRows.HasValue && maxRows.Value > 0
            ? new List<ExecutionRow>(maxRows.Value)
            : [];

        root.Open();

        try
        {
            while (!maxRows.HasValue || result.Count < maxRows.Value)
            {
                var row = root.GetNextRow();
                if (row == null)
                {
                    break;
                }

                result.Add(row);
            }
        }
        finally
        {
            root.Close();
        }

        return result;
    }
}
