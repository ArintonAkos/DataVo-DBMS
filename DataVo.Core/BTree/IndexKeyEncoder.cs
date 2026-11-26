using System.Text;
using System.Globalization;
using DataVo.Core.Compat;
using DataVo.Core.Exceptions;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.Utils;

namespace DataVo.Core.BTree;

/// <summary>
/// Builds the logical string keys used by in-memory and LSM secondary indexes from dictionary rows
/// or typed cells. Composite keys join their parts with <see cref="CompositeKeySeparator"/>.
/// </summary>
public static class IndexKeyEncoder
{
    /// <summary>
    /// The delimiter used when building composite key strings from multiple attribute values.
    /// </summary>
    public const string CompositeKeySeparator = "##";

    /// <summary>
    /// Builds a logical key string from a row and a sequence of indexed attributes.
    /// </summary>
    /// <param name="row">The row values keyed by column name.</param>
    /// <param name="attributes">The indexed attributes, in key order.</param>
    /// <returns>A single-column key or a composite key joined with <see cref="CompositeKeySeparator"/>.</returns>
    public static string BuildKeyString(Dictionary<string, object?> row, IEnumerable<string> attributes)
    {
        return string.Join(CompositeKeySeparator, attributes.Select(attr => NormalizeValue(row[attr])));
    }

    /// <summary>
    /// Builds the logical index key string for a single column from a row dictionary when that column is
    /// present and non-null, without allocating an attribute array.
    /// </summary>
    /// <param name="row">The row values keyed by column name.</param>
    /// <param name="columnName">The indexed column name.</param>
    /// <param name="key">The normalized index key string when present.</param>
    /// <returns><see langword="true"/> when the row has a non-null value for <paramref name="columnName"/>.</returns>
    /// <example>
    /// <code>
    /// if (IndexKeyEncoder.TryBuildSingleColumnKey(row, "Id", out string key)) { /* use key */ }
    /// </code>
    /// </example>
    public static bool TryBuildSingleColumnKey(IReadOnlyDictionary<string, object?> row, string columnName, out string key)
    {
        if (TryGetColumnValue(row, columnName, out object? value) && value is not null && value is not DBNull)
        {
            key = NormalizeValue(value);
            return true;
        }

        key = string.Empty;
        return false;
    }

    private static bool TryGetColumnValue(IReadOnlyDictionary<string, object?> row, string columnName, out object? value)
    {
        if (row.TryGetValue(columnName, out value))
        {
            return true;
        }

        foreach (KeyValuePair<string, object?> pair in row)
        {
            if (string.Equals(pair.Key, columnName, StringComparison.OrdinalIgnoreCase))
            {
                value = pair.Value;
                return true;
            }
        }

        value = null;
        return false;
    }

    /// <summary>
    /// Gets whether a row has a value for every indexed attribute. NULL keys are never indexed: a row with a
    /// NULL (or absent) key column gets no entry in a scalar index, on every write and rebuild path.
    /// </summary>
    /// <param name="row">The row values keyed by column name.</param>
    /// <param name="attributes">The indexed attributes.</param>
    /// <returns><see langword="true"/> when the row has an index entry.</returns>
    public static bool HasCompleteKey(IReadOnlyDictionary<string, object?> row, IReadOnlyList<string> attributes)
    {
        for (int i = 0; i < attributes.Count; i++)
        {
            if (!row.TryGetValue(attributes[i], out object? value) || value is null || value is DBNull)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Gets whether typed cells have a non-NULL value for every indexed attribute (see <see cref="HasCompleteKey(IReadOnlyDictionary{string, object?}, IReadOnlyList{string})"/>).</summary>
    internal static bool HasCompleteKey(ReactiveRowSchema schema, ReadOnlySpan<CellValue> cells, IReadOnlyList<string> attributes)
    {
        for (int i = 0; i < attributes.Count; i++)
        {
            if (!schema.TryGetOrdinal(attributes[i], out int ordinal) || cells[ordinal].Type == CellType.Null)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Builds a logical key string from typed cells and a sequence of indexed attributes, producing a key
    /// identical to the dictionary <see cref="BuildKeyString(Dictionary{string, object?}, IEnumerable{string})"/>
    /// overload. This is the typed-storage path (no dictionary materialization).
    /// </summary>
    /// <param name="schema">The column layout describing <paramref name="cells"/>.</param>
    /// <param name="cells">The row's cells, in schema order.</param>
    /// <param name="attributes">The indexed attributes, in key order.</param>
    /// <returns>A single-column key or a composite key joined with <see cref="CompositeKeySeparator"/>.</returns>
    internal static string BuildKeyString(
        ReactiveRowSchema schema,
        ReadOnlySpan<CellValue> cells,
        IReadOnlyList<string> attributes)
    {
        if (attributes.Count == 1)
        {
            return NormalizeCell(GetCell(schema, cells, attributes[0]));
        }

        var builder = new StringBuilder();
        for (int i = 0; i < attributes.Count; i++)
        {
            if (i > 0)
            {
                builder.Append(CompositeKeySeparator);
            }

            builder.Append(NormalizeCell(GetCell(schema, cells, attributes[i])));
        }

        return builder.ToString();
    }

    /// <summary>Builds the logical key string of one typed cell, as a single-column index stores it.</summary>
    /// <param name="cell">The cell.</param>
    /// <returns>The same key <see cref="BuildKeyString(ReactiveRowSchema, ReadOnlySpan{CellValue}, IReadOnlyList{string})"/> gives for one column.</returns>
    internal static string BuildCellKeyString(CellValue cell) => NormalizeCell(cell);

    private static CellValue GetCell(ReactiveRowSchema schema, ReadOnlySpan<CellValue> cells, string columnName)
    {
        if (!schema.TryGetOrdinal(columnName, out int ordinal))
        {
            throw new BindingException($"Column {columnName} doesn't exist in typed row schema!");
        }

        return cells[ordinal];
    }

    // Preserve dictionary-path key compatibility without boxing typed scalar cells or allocating transient
    // vector arrays for numeric values. The legacy dictionary path treats parseable numeric strings as a
    // single-element vector, so typed numeric keys keep the same "[n]" logical key shape.
    private static string NormalizeCell(CellValue cell) => cell.Type switch
    {
        CellType.Null => string.Empty,
        CellType.Int32 => FormatSingleElementVector(cell.AsInt32()),
        CellType.Int64 => FormatSingleElementVector(cell.AsInt64()),
        CellType.Double => FormatSingleElementVector(cell.AsDouble()),
        CellType.Decimal => FormatSingleElementVector(cell.AsDecimal()),
        CellType.Boolean => cell.AsBoolean().ToString(),
        CellType.String => NormalizeStringCell(cell.AsString()),
        CellType.Date => cell.AsDate().ToString(),
        CellType.DateTime => DataVoDateTime.Format(cell.AsUtcTicks()),
        CellType.Vector => SerializeVector(cell.AsVectorReadOnlySpan()),
        CellType.Guid => cell.AsGuid().ToString("D"),
        _ => string.Empty
    };

    private static string NormalizeStringCell(string? value)
    {
        if (value == null)
        {
            return string.Empty;
        }

        return VectorParser.TryParseVector(value, out float[] vector)
            ? VectorParser.SerializeVector(vector)
            : value;
    }

    private static string FormatSingleElementVector(int value) =>
        ScalarKeyFormatter.FormatBracketed(value, CultureInfo.InvariantCulture);

    private static string FormatSingleElementVector(long value) =>
        ScalarKeyFormatter.FormatBracketed(value, CultureInfo.InvariantCulture);

    // Index keys must not depend on the thread's culture: CurrentCulture is per thread (ASP.NET sets it
    // per request), and a comma decimal separator turned 1.25 into the two-element key "[1,25]".
    private static string FormatSingleElementVector(double value) =>
        ScalarKeyFormatter.FormatBracketed(value, CultureInfo.InvariantCulture);

    private static string FormatSingleElementVector(decimal value) =>
        ScalarKeyFormatter.FormatBracketed(value, CultureInfo.InvariantCulture);

    private static string SerializeVector(ReadOnlySpan<float> vector)
    {
        if (vector.Length == 0)
        {
            return "[]";
        }

        var builder = new StringBuilder();
        builder.Append('[');
        for (int i = 0; i < vector.Length; i++)
        {
            if (i > 0)
            {
                builder.Append(',');
            }

            builder.Append(vector[i].ToString(CultureInfo.InvariantCulture));
        }

        builder.Append(']');
        return builder.ToString();
    }

    private static string NormalizeValue(object? value)
    {
        if (value == null)
        {
            return string.Empty;
        }

        if (value is Guid guid)
        {
            return guid.ToString("D");
        }

        // A DATETIME keys on its canonical UTC text (fixed width, so key order is time order), as NormalizeCell does.
        if (value is DateTime dateTime)
        {
            return DataVoDateTime.Format(dateTime);
        }

        if (value is DateTimeOffset offset)
        {
            return DataVoDateTime.Format(offset.UtcTicks);
        }

        // Numbers are formatted exactly like the typed path (NormalizeCell), culture-invariantly. Going
        // through value.ToString() used the thread's culture and re-parsed the text as a float vector,
        // which lost precision and split "1,25" into two elements.
        switch (value)
        {
            case int intValue:
                return FormatSingleElementVector(intValue);
            case long longValue:
                return FormatSingleElementVector(longValue);
            case short shortValue:
                return FormatSingleElementVector((int)shortValue);
            case byte byteValue:
                return FormatSingleElementVector((int)byteValue);
            case double doubleValue:
                return FormatSingleElementVector(doubleValue);
            case float floatValue:
                return FormatSingleElementVector((double)floatValue);
            case decimal decimalValue:
                return FormatSingleElementVector(decimalValue);
        }

        if (VectorParser.TryCoerceToVector(value, out float[] vector))
        {
            return VectorParser.SerializeVector(vector);
        }

        return value.ToString() ?? string.Empty;
    }
}
