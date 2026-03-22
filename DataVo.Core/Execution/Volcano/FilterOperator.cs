namespace DataVo.Core.Execution.Volcano;

/// <summary>
/// Unary operator that streams only rows satisfying a predicate.
/// </summary>
public sealed class FilterOperator : IQueryOperator
{
    private readonly IQueryOperator _source;
    private readonly Func<ExecutionRow, bool> _predicate;

    /// <summary>
    /// Initializes a filter operator over a source stream.
    /// </summary>
    public FilterOperator(IQueryOperator source, Func<ExecutionRow, bool> predicate)
    {
        _source = source;
        _predicate = predicate;
    }

    /// <inheritdoc />
    public void Open()
    {
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

            if (_predicate(row))
            {
                return row;
            }
        }
    }

    /// <inheritdoc />
    public void Close()
    {
        _source.Close();
    }
}
