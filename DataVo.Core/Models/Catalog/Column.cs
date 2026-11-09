using DataVo.Core.Contracts;
using DataVo.Core.Utils;
using System.Globalization;

namespace DataVo.Core.Models.Catalog;

/// <summary>
/// Represents a parsed column with its schema definition and runtime value.
/// Used during query execution and data manipulation.
/// </summary>
/// <example>
/// <code>
/// var col = new Column { Name = "Age", Type = "INT", Value = "30" };
/// </code>
/// </example>
public class Column : IColumn
{
    /// <summary>Gets or sets the column name.</summary>
    public string Name { get; set; } = null!;

    /// <summary>Gets or sets the data type of the column (e.g., INT, VARCHAR, DATE).</summary>
    public string Type { get; set; } = null!;

    /// <summary>Gets or sets whether the column rejects NULL (declared NOT NULL).</summary>
    public bool IsNotNull { get; set; }

    /// <summary>Gets or sets the max length for variable-length types like VARCHAR.</summary>
    public int Length { get; set; }

    /// <summary>Gets or sets the raw string representation of the column's value.</summary>
    public string? Value { get; set; }

    /// <summary>Gets or sets the raw string representation of the column's default value.</summary>
    public string? DefaultValue { get; set; }

    /// <summary>
    /// Gets the strongly-typed value parsed from <see cref="Value"/> based on the column's <see cref="Type"/>.
    /// Parses to <see cref="int"/>, <see cref="double"/>, <see cref="bool"/>, or <see cref="DataVoDate"/> where applicable.
    /// </summary>
    public object? ParsedValue
    {
        get => ParseTypedValue(Value);
    }

    /// <summary>
    /// Gets the raw string type of the column.
    /// </summary>
    /// <returns>The raw data type string.</returns>
    public string RawType()
    {
        return Type;
    }

    /// <summary>
    /// Gets the strongly-typed default value parsed from <see cref="DefaultValue"/> based on the column's <see cref="Type"/>.
    /// </summary>
    public object? ParsedDefaultValue
    {
        get => ParseTypedValue(DefaultValue);
    }

    private object? ParseTypedValue(string? rawValue)
    {
        if (rawValue == null || rawValue.Equals("NULL", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        return Type.ToUpperInvariant() switch
        {
            "VARCHAR" => rawValue, // length is enforced by ColumnLengthRule; never truncate

            "DATE" => TryParseDate(rawValue, out DataVoDate parsedDate) ? parsedDate : rawValue,
            "GUID" => Guid.TryParse(rawValue, out Guid parsedGuid) ? parsedGuid : rawValue,
            "BIT" => bool.TryParse(rawValue, out bool parsedBit) ? parsedBit : rawValue,
            "INT" => int.TryParse(rawValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out int parsedInt) ? parsedInt : rawValue,
            "BIGINT" => long.TryParse(rawValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsedBigInt) ? parsedBigInt : rawValue,
            "FLOAT" => TryParseFloatingPoint(rawValue, out double parsedFloat) ? parsedFloat : rawValue,
            "DOUBLE" => TryParseFloatingPoint(rawValue, out double parsedDouble) ? parsedDouble : rawValue,
            "VECTOR" => VectorParser.TryParseVector(rawValue, out float[] parsedVector) ? parsedVector : rawValue,
            _ => rawValue,
        };
    }

    private static bool TryParseDate(string input, out DataVoDate parsedDate)
    {
        return DataVoDate.TryParse(input, CultureInfo.InvariantCulture, DateTimeStyles.None, out parsedDate)
            || DataVoDate.TryParse(input, CultureInfo.CurrentCulture, DateTimeStyles.None, out parsedDate);
    }

    private static bool TryParseFloatingPoint(string input, out double parsedFloat)
    {
        return double.TryParse(input, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out parsedFloat)
            || double.TryParse(input, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.CurrentCulture, out parsedFloat);
    }
}
