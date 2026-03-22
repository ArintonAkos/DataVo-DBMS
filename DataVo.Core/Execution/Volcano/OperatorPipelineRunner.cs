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
    {
        var result = new List<ExecutionRow>();
        root.Open();

        try
        {
            while (true)
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
