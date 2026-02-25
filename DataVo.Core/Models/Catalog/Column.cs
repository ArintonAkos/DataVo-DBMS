using DataVo.Core.Contracts;

namespace DataVo.Core.Models.Catalog;

public class Column : IColumn
{
    public string Name { get; set; } = null!;
    public string Type { get; set; } = null!;
    public int Length { get; set; }
    public string? Value { get; set; }

    public dynamic? ParsedValue
    {
        get
        {
            if (Value == null)
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
}