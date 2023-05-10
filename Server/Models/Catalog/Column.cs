namespace Server.Models.Catalog;

public class Column
{
    public string Name { get; set; }
    public string Type { get; set; }
    public int Length { get; set; }
    public string? Value { get; set; }

    public dynamic? ParsedValue
    {
        get
        {
            if (Value == null) return null;

            try
            {
                return Type switch
                {
                    "Varchar" => Length < Value.Length ? Value[..Length] : Value,
                    "Date" => DateOnly.Parse(Value),
                    "Bit" => bool.Parse(Value),
                    "Int" => int.Parse(Value),
                    "Float" => float.Parse(Value),
                    _ => null
                };
            }
            catch (Exception)
            {
                return null;
            }
        }
    }
}