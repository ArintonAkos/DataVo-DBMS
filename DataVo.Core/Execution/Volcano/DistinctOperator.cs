namespace DataVo.Core.Execution.Volcano;

/// <summary>
/// Unary operator that emits only the first row for each distinct key.
/// </summary>
public sealed class DistinctOperator : IQueryOperator
{
    private readonly IQueryOperator _source;
    private readonly Func<ExecutionRow, string> _keySelector;
    private readonly HashSet<string> _seen = [];

    /// <summary>
    /// Initializes a distinct operator over a source stream.
    /// </summary>
    public DistinctOperator(IQueryOperator source, Func<ExecutionRow, string> keySelector)
    {
        _source = source;
        _keySelector = keySelector;
    }

    /// <inheritdoc />
    public void Open()
    {
        _seen.Clear();
        _source.Open();
    }

    /// <inheritdoc />
    public ExecutionRow? GetNextRow()
    {
        while (true)
        {
            var row = _source.GetNextRow();
            if (row == null)
            {
                return null;
            }

            string key = _keySelector(row);
            if (_seen.Add(key))
            {
                return row;
            }
        }
    }

    /// <inheritdoc />
    public void Close()
    {
        _source.Close();
        _seen.Clear();
    }
}
