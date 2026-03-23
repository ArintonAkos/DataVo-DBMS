namespace DataVo.Core.Execution.Volcano;

/// <summary>
/// Typed row carrier for Volcano hot paths that operate on object-valued payloads.
/// </summary>
public sealed class TypedExecutionRow
{
    public TypedExecutionRow(long rowId, Dictionary<string, object?> values)
    {
        RowId = rowId;
        Values = values;
    }

    public long RowId { get; }

    public Dictionary<string, object?> Values { get; }
}
