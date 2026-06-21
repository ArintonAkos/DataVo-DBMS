using System.Globalization;
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
    {
        switch (value)
        {
            case bool b:
                writer.WriteBooleanValue(b);
                break;
            case string s:
                writer.WriteStringValue(s);
                break;
            case int i:
                writer.WriteNumberValue(i);
                break;
            case long l:
                writer.WriteNumberValue(l);
                break;
            case short sh:
                writer.WriteNumberValue(sh);
                break;
            case byte bt:
                writer.WriteNumberValue(bt);
                break;
            case uint ui:
                writer.WriteNumberValue(ui);
                break;
            case ulong ul:
                writer.WriteNumberValue(ul);
                break;
            case float f:
                writer.WriteNumberValue(f);
                break;
            case double d:
                writer.WriteNumberValue(d);
                break;
            case decimal m:
                writer.WriteNumberValue(m);
                break;
            case DateOnly dateOnly:
                writer.WriteStringValue(dateOnly.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
                break;
            case DateTime dateTime:
                writer.WriteStringValue(dateTime.ToString("o", CultureInfo.InvariantCulture));
                break;
            case Guid guid:
                writer.WriteStringValue(guid.ToString());
                break;
            // Covers both the base64 vector envelope (Dictionary<string, object>) and nested object maps;
            // object and object? are the same runtime type, so one case handles both.
            case IDictionary<string, object?> map:
                writer.WriteStartObject();
                foreach (KeyValuePair<string, object?> pair in map)
                {
                    writer.WritePropertyName(pair.Key);
                    WriteNullable(writer, pair.Value, options);
                }

                writer.WriteEndObject();
                break;
            case System.Collections.IEnumerable sequence:
                writer.WriteStartArray();
                foreach (object? item in sequence)
                {
                    WriteNullable(writer, item, options);
                }

                writer.WriteEndArray();
                break;
            default:
                throw new JsonException($"Unsupported WAL value type '{value.GetType().Name}'.");
        }
    }

    private void WriteNullable(Utf8JsonWriter writer, object? value, JsonSerializerOptions options)
    {
        if (value is null)
        {
            writer.WriteNullValue();
            return;
        }

        Write(writer, value, options);
    }
}
