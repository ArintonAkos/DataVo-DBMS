using DataVo.Core.Contracts;

namespace DataVo.Core.Models.Catalog;

public class Column : IColumn
{
    public string Name { get; set; } = null!;
    public string Type { get; set; } = null!;
    public int Length { get; set; }
    public string? Value { get; set; }
    public string? DefaultValue { get; set; }

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

    public string RawType()
    {
        return Type;
    }

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