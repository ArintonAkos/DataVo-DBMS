namespace DataVo.Core.Execution.Volcano;

/// <summary>
/// Streaming execution row used by Volcano operators.
/// </summary>
public sealed class ExecutionRow
{
    /// <summary>
    /// Initializes a new streamed execution row.
    /// </summary>
    public ExecutionRow(long rowId, Dictionary<string, object?> values)
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
    public Dictionary<string, object?> Values { get; }

    /// <summary>
    /// Gets or sets a value by column key.
    /// </summary>
    public object? this[string key]
    {
        get => Values[key];
        set => Values[key] = value;
    }

    /// <summary>
    /// Converts this dynamic-backed row to a typed object-backed row carrier.
    /// </summary>
    public TypedExecutionRow ToTyped()
    {
        var typedValues = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var entry in Values)
        {
            typedValues[entry.Key] = entry.Value;
        }

        return new TypedExecutionRow(RowId, typedValues);
    }

    /// <summary>
    /// Creates an execution row from a typed object-backed row carrier.
    /// </summary>
    public static ExecutionRow FromTyped(TypedExecutionRow typed)
    {
        var values = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var entry in typed.Values)
        {
            values[entry.Key] = entry.Value;
        }

        return new ExecutionRow(typed.RowId, values);
    }
}
