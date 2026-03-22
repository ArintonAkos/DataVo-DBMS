namespace DataVo.Core.Execution.Volcano;

/// <summary>
/// Streaming execution row used by Volcano operators.
/// </summary>
public sealed class ExecutionRow
{
    /// <summary>
    /// Initializes a new streamed execution row.
    /// </summary>
    public ExecutionRow(long rowId, Dictionary<string, dynamic> values)
    {
        RowId = rowId;
        Values = values;
    }

    /// <summary>
    /// Gets the stable row identifier.
    /// </summary>
    public long RowId { get; }

    /// <summary>
    /// Gets the row values.
    /// </summary>
    public Dictionary<string, dynamic> Values { get; }

    /// <summary>
    /// Gets or sets a value by column key.
    /// </summary>
    public dynamic this[string key]
    {
        get => Values[key];
        set => Values[key] = value;
    }
}
