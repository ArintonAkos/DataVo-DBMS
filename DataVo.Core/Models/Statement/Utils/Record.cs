namespace DataVo.Core.Models.Statement.Utils;

/// <summary>
/// Represents a physical row record paired with its row identifier.
/// </summary>
/// <param name="rowId">The row identifier.</param>
/// <param name="values">Row values keyed by column name.</param>
public class Record(long rowId, Dictionary<string, object?> values)
{
    /// <summary>
    /// Gets or sets the row identifier.
    /// </summary>
    public long RowId { get; set; } = rowId;

    /// <summary>
    /// Gets or sets the row values.
    /// </summary>
    public Dictionary<string, object?> Values { get; set; } = values;

    /// <summary>
    /// Gets or sets a value by column name.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    /// <returns>The row value.</returns>
    public object? this[string columnName]
    {
        get => Values[columnName];
        set => Values[columnName] = value;
    }

    /// <summary>
    /// Determines whether the row contains a value for the specified key.
    /// </summary>
    /// <param name="key">The column name.</param>
    /// <returns><see langword="true"/> when present; otherwise <see langword="false"/>.</returns>
    public bool ContainsKey(string key) => Values.ContainsKey(key);
}
