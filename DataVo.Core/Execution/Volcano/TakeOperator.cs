namespace DataVo.Core.Execution.Volcano;

/// <summary>
/// Unary operator that yields at most a fixed number of rows.
/// </summary>
public sealed class TakeOperator : IQueryOperator
{
    private readonly IQueryOperator _source;
    private readonly int _take;
    private int _yielded;

    /// <summary>
    /// Initializes a take operator over a source stream.
    /// </summary>
    public TakeOperator(IQueryOperator source, int take)
    {
        _source = source;
        _take = Math.Max(0, take);
        _yielded = 0;
    }

    /// <inheritdoc />
    public void Open()
    {
        _yielded = 0;
        _source.Open();
    }

    /// <inheritdoc />
    public ExecutionRow? GetNextRow()
    {
        if (_yielded >= _take)
        {
            return null;
        }

        var row = _source.GetNextRow();
        if (row == null)
        {
            return null;
        }

        _yielded++;
        return row;
    }

    /// <inheritdoc />
    public void Close()
    {
        _source.Close();
    }
}
