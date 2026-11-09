namespace DataVo.Core;

/// <summary>
/// One row of a streamed query. Each row owns its values, so it stays valid after the stream moves on.
/// </summary>
/// <example>
/// <code>
/// foreach (DataVoRow row in context.StreamQuery("SELECT Id, Name FROM Players"))
/// {
///     Console.WriteLine($"{row["Id"]}: {row["Name"]}");
/// }
/// </code>
/// </example>
public sealed class DataVoRow
{
    private readonly DataVoRowSchema _schema;
    private readonly object?[] _values;

    internal DataVoRow(DataVoRowSchema schema, object?[] values)
    {
        _schema = schema;
        _values = values;
    }

    /// <summary>Gets the column names, in SELECT-list order.</summary>
    public IReadOnlyList<string> Fields => _schema.Fields;

    /// <summary>Gets the value at a column position.</summary>
    /// <param name="ordinal">The zero-based position.</param>
    public object? this[int ordinal] => _values[ordinal];

    /// <summary>Gets the value of a named column. Names match without regard to case.</summary>
    /// <param name="column">The column name.</param>
    /// <exception cref="KeyNotFoundException">The query did not return that column.</exception>
    public object? this[string column] => _schema.TryGetOrdinal(column, out int ordinal)
        ? _values[ordinal]
        : throw new KeyNotFoundException($"The row has no column named '{column}'.");

    /// <summary>Copies the row into a new dictionary keyed by column name.</summary>
    /// <returns>The row's values.</returns>
    public Dictionary<string, object?> ToDictionary()
    {
        var copy = new Dictionary<string, object?>(_values.Length);
        for (int i = 0; i < _values.Length; i++)
        {
            copy[_schema.Fields[i]] = _values[i];
        }

        return copy;
    }
}

/// <summary>The column names shared by every row of one stream.</summary>
internal sealed class DataVoRowSchema
{
    private readonly Dictionary<string, int> _ordinals = new(StringComparer.OrdinalIgnoreCase);

    public DataVoRowSchema(IReadOnlyList<string> fields)
    {
        Fields = fields;
        for (int i = 0; i < fields.Count; i++)
        {
            _ordinals.TryAdd(fields[i], i);
        }
    }

    public IReadOnlyList<string> Fields { get; }

    public bool TryGetOrdinal(string column, out int ordinal) => _ordinals.TryGetValue(column, out ordinal);
}
