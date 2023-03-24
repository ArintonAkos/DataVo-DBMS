
namespace Server.Models.Catalog
{
    public class Column
    {
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
                        "Varchar" => (Length < Value.Length) ? Value[..Length] : Value,
                        "Int" => int.Parse(Value),
                        "Float" => float.Parse(Value),
                        _ => throw new FormatException("Unsupported type!")
                    };
                }
                catch (Exception) 
                {
                    return null;
                }
            } 
        }
    }
}
