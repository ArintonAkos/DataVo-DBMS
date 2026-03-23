namespace DataVo.Core.Execution.Volcano;

/// <summary>
/// Unary operator that skips a fixed number of rows before yielding.
/// </summary>
public sealed class SkipOperator : IQueryOperator
{
    private readonly IQueryOperator _source;
    private readonly int _skip;
    private int _skipped;

    /// <summary>
    /// Initializes a skip operator over a source stream.
    /// </summary>
    public SkipOperator(IQueryOperator source, int skip)
    {
        _source = source;
        _skip = Math.Max(0, skip);
        _skipped = 0;
    }

    /// <inheritdoc />
    public void Open()
    {
        _skipped = 0;
        _source.Open();
    }

    /// <inheritdoc />
    public ExecutionRow? GetNextRow()
    {
        while (_skipped < _skip)
        {
            var skippedRow = _source.GetNextRow();
            if (skippedRow == null)
            {
                return null;
            }

            _skipped++;
        }

        return _source.GetNextRow();
    }

    /// <inheritdoc />
    public void Close()
    {
        _source.Close();
    }
}
