using System.Text.Json;
using System.Text.Json.Serialization;

namespace DataVo.Core.Serialization;

/// <summary>
/// JSON form of a <see cref="RowKey"/>: an integer key is a number (the format integer row ids always
/// had, so existing files stay readable), any other key a GUID string.
/// </summary>
public sealed class RowKeyJsonConverter : JsonConverter<RowKey>
{
    public override RowKey Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        reader.TokenType switch
        {
            JsonTokenType.Number => RowKey.FromInt64(reader.GetInt64()),
            JsonTokenType.String => RowKey.Parse(reader.GetString()!),
            _ => throw new JsonException($"Expected a number or a string for a row key, got {reader.TokenType}."),
        };

    public override void Write(Utf8JsonWriter writer, RowKey value, JsonSerializerOptions options)
    {
        if (value.IsInt64)
        {
            writer.WriteNumberValue(value.AsInt64());
        }
        else
        {
            writer.WriteStringValue(value.AsGuid().ToString("D"));
        }
    }
}
