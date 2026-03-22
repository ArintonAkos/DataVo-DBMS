using DataVo.Core.Contracts;

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
    
    /// <summary>Gets or sets the max length for variable-length types like VARCHAR.</summary>
    public int Length { get; set; }
    
    /// <summary>Gets or sets the raw string representation of the column's value.</summary>
    public string? Value { get; set; }
    
    /// <summary>Gets or sets the raw string representation of the column's default value.</summary>
    public string? DefaultValue { get; set; }

    /// <summary>
    /// Gets the strongly-typed value parsed from <see cref="Value"/> based on the column's <see cref="Type"/>.
    /// Parses to <see cref="int"/>, <see cref="double"/>, <see cref="bool"/>, or <see cref="DateOnly"/> where applicable.
    /// </summary>
    public dynamic? ParsedValue
    {
        get
        {
            if (Value == null || Value.Equals("NULL", StringComparison.OrdinalIgnoreCase))
            {
                return null;
            }

            try
            {
                return Type.ToUpperInvariant() switch
                {
                    "VARCHAR" => (Length > 0 && Length < Value.Length) ? Value[..Length] : Value,
                    "DATE" => DateOnly.Parse(Value),
                    "BIT" => bool.Parse(Value),
                    "INT" => int.Parse(Value),
                    "FLOAT" => double.Parse(Value, System.Globalization.CultureInfo.InvariantCulture),
                    _ => Value,
                };
            }
            catch (Exception)
            {
                return null;
            }
        }
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
    public dynamic? ParsedDefaultValue
    {
        get
        {
            if (DefaultValue == null || DefaultValue.Equals("NULL", StringComparison.OrdinalIgnoreCase))
            {
                return null;
            }

            try
            {
                return Type.ToUpperInvariant() switch
                {
                    "VARCHAR" => (Length > 0 && Length < DefaultValue.Length) ? DefaultValue[..Length] : DefaultValue,
                    "DATE" => DateOnly.Parse(DefaultValue),
                    "BIT" => bool.Parse(DefaultValue),
                    "INT" => int.Parse(DefaultValue),
                    "FLOAT" => double.Parse(DefaultValue, System.Globalization.CultureInfo.InvariantCulture),
                    _ => DefaultValue,
                };
            }
            catch (Exception)
            {
                return null;
            }
        }
    }
}