namespace DataVo.Core.Execution.Volcano;

/// <summary>
/// Typed row carrier for Volcano hot paths that operate on object-valued payloads.
/// </summary>
public sealed class TypedExecutionRow
{
    /// <summary>
    /// Initializes a typed execution row.
    /// </summary>
    /// <param name="rowId">The row identifier.</param>
    /// <param name="values">Row values keyed by column name.</param>
    public TypedExecutionRow(long rowId, Dictionary<string, object?> values)
    {
        RowId = rowId;
        Values = values;
    }

    /// <summary>
    /// Gets the row identifier.
    /// </summary>
    public long RowId { get; }

    /// <summary>
    /// Gets the row values.
    /// </summary>
    public Dictionary<string, object?> Values { get; }
}
