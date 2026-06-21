using System.Globalization;
using System.Text.Json;

namespace DataVo.Core.Serialization;

/// <summary>
/// Shared, reflection-free (Native-AOT-safe) writer for heterogeneous <c>object?</c> values. Writes the
/// constrained value set the engine persists (primitives, ISO date/Guid strings, nested string-keyed maps,
/// and arrays) by switching on the runtime type. Used by the WAL and Volcano-spill object converters so the
/// write path is defined in exactly one place.
/// </summary>
internal static class PolymorphicJsonWriter
{
    public static void WriteValue(Utf8JsonWriter writer, object? value, JsonSerializerOptions options)
    {
        switch (value)
        {
            case null:
                writer.WriteNullValue();
                break;
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
                    WriteValue(writer, pair.Value, options);
                }

                writer.WriteEndObject();
                break;
            case System.Collections.IEnumerable sequence:
                writer.WriteStartArray();
                foreach (object? item in sequence)
                {
                    WriteValue(writer, item, options);
                }

                writer.WriteEndArray();
                break;
            default:
                throw new JsonException($"Unsupported JSON value type '{value.GetType().Name}'.");
        }
    }
}

/// <summary>
/// AOT-safe <see cref="object"/> converter for spill/snapshot payloads whose values must round-trip with
/// the same shape the previous reflection-based <see cref="System.Text.Json"/> path produced: reads each
/// value as a <see cref="JsonElement"/> (the consumers post-process those), and writes by runtime type via
/// <see cref="PolymorphicJsonWriter"/>.
/// </summary>
internal sealed class JsonElementObjectConverter : System.Text.Json.Serialization.JsonConverter<object>
{
    public override object Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using JsonDocument document = JsonDocument.ParseValue(ref reader);
        return document.RootElement.Clone();
    }

    public override void Write(Utf8JsonWriter writer, object value, JsonSerializerOptions options)
        => PolymorphicJsonWriter.WriteValue(writer, value, options);
}

/// <summary>
/// Shared source-gen context instance for Volcano disk-spill payloads (<c>TypedExecutionRow</c>), whose
/// <c>object?</c> values use <see cref="JsonElementObjectConverter"/> so they round-trip as
/// <see cref="JsonElement"/> exactly as the prior reflection-based path did.
/// </summary>
internal static class SpillJson
{
    public static readonly DataVoJsonContext Context =
        new(new JsonSerializerOptions { Converters = { new JsonElementObjectConverter() } });
}
