using System.Text.Json;
using System.Text.Json.Serialization;

namespace DataVo.Core.Serialization;

/// <summary>
/// AOT-safe System.Text.Json converter for the heterogeneous <c>object?</c> values stored in WAL row
/// payloads. It writes by runtime type (the constrained set the WAL persists: primitives, ISO date/Guid
/// strings, the base64 vector-envelope dictionary, and nested arrays) and reads JSON back into plain CLR
/// values (<see cref="long"/>/<see cref="double"/>/<see cref="string"/>/<see cref="bool"/>,
/// <c>Dictionary&lt;string, object?&gt;</c>, <c>List&lt;object?&gt;</c>) — matching the previous Newtonsoft
/// <c>JValue</c>/<c>JObject</c>/<c>JArray</c> behavior that <c>WalEntry.NormalizeValue</c> post-processes.
/// No reflection or runtime code generation, so it is Native-AOT compatible.
/// </summary>
internal sealed class WalObjectConverter : JsonConverter<object>
{
    public override object? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        switch (reader.TokenType)
        {
            case JsonTokenType.True:
                return true;
            case JsonTokenType.False:
                return false;
            case JsonTokenType.Null:
                return null;
            case JsonTokenType.String:
                return reader.GetString();
            case JsonTokenType.Number:
                // Match Newtonsoft: integral numbers -> long, everything else -> double.
                return reader.TryGetInt64(out long integral) ? integral : reader.GetDouble();
            case JsonTokenType.StartObject:
            {
                var dictionary = new Dictionary<string, object?>(StringComparer.Ordinal);
                while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
                {
                    string propertyName = reader.GetString()!;
                    reader.Read();
                    dictionary[propertyName] = Read(ref reader, typeof(object), options);
                }

                return dictionary;
            }

            case JsonTokenType.StartArray:
            {
                var list = new List<object?>();
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                {
                    list.Add(Read(ref reader, typeof(object), options));
                }

                return list;
            }

            default:
                throw new JsonException($"Unexpected WAL JSON token '{reader.TokenType}'.");
        }
    }

    public override void Write(Utf8JsonWriter writer, object value, JsonSerializerOptions options)
        => PolymorphicJsonWriter.WriteValue(writer, value, options);
}
