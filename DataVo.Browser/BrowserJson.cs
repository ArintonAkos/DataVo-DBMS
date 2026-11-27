using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using DataVo.Core.Contracts.Results;

namespace DataVo.Browser;

/// <summary>Runtime flags the JS host passes to the engine (<c>getRuntimeFlags()</c>).</summary>
internal sealed record BrowserRuntimeFlagsPayload(
    [property: JsonPropertyName("forceVectorFallback")] bool? ForceVectorFallback);

/// <summary>Minimal capability payload used when the JS host does not report its own.</summary>
internal sealed record BrowserRuntimeCapabilities(
    [property: JsonPropertyName("storageBackend")] string StorageBackend,
    [property: JsonPropertyName("engineInitialized")] bool EngineInitialized,
    [property: JsonPropertyName("forceVectorFallback")] bool ForceVectorFallback);

/// <summary>
/// Source-generated (reflection-free) JSON metadata for the browser interop payloads. Result cells are
/// <see cref="object"/>-typed, so <see cref="BrowserCellJsonConverter"/> writes them by runtime type.
/// </summary>
[JsonSourceGenerationOptions(Converters = [typeof(BrowserCellJsonConverter)])]
[JsonSerializable(typeof(List<QueryResult>))]
[JsonSerializable(typeof(BrowserRuntimeFlagsPayload))]
[JsonSerializable(typeof(BrowserRuntimeCapabilities))]
internal sealed partial class BrowserJsonContext : JsonSerializerContext;

/// <summary>
/// Writes <c>QueryResult.Data</c> cells by their runtime type without reflection. Unknown types fall back
/// to their invariant string form, and non-finite floats to strings, so serialization never throws.
/// </summary>
internal sealed class BrowserCellJsonConverter : JsonConverter<object>
{
    public override object? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        throw new NotSupportedException("Browser result cells are write-only.");

    public override void Write(Utf8JsonWriter writer, object value, JsonSerializerOptions options)
    {
        switch (value)
        {
            case string s: writer.WriteStringValue(s); break;
            case bool b: writer.WriteBooleanValue(b); break;
            case int i: writer.WriteNumberValue(i); break;
            case long l: writer.WriteNumberValue(l); break;
            case short sh: writer.WriteNumberValue(sh); break;
            case byte by: writer.WriteNumberValue(by); break;
            case decimal m: writer.WriteNumberValue(m); break;
            case double d: WriteFloat(writer, d); break;
            case float f: WriteFloat(writer, f); break;
            case DateTime dt: writer.WriteStringValue(dt); break;
            case DateTimeOffset dto: writer.WriteStringValue(dto); break;
            case Guid g: writer.WriteStringValue(g); break;
            case TimeSpan ts: writer.WriteStringValue(ts.ToString("c", CultureInfo.InvariantCulture)); break;
            case byte[] bytes: writer.WriteBase64StringValue(bytes); break;
            case float[] vector:
                writer.WriteStartArray();
                foreach (float component in vector)
                {
                    WriteFloat(writer, component);
                }
                writer.WriteEndArray();
                break;
            case DBNull: writer.WriteNullValue(); break;
            case IFormattable formattable: writer.WriteStringValue(formattable.ToString(null, CultureInfo.InvariantCulture)); break;
            default: writer.WriteStringValue(value.ToString()); break;
        }
    }

    private static void WriteFloat(Utf8JsonWriter writer, double value)
    {
        if (double.IsFinite(value))
        {
            writer.WriteNumberValue(value);
        }
        else
        {
            writer.WriteStringValue(value.ToString(CultureInfo.InvariantCulture));
        }
    }

    private static void WriteFloat(Utf8JsonWriter writer, float value)
    {
        if (float.IsFinite(value))
        {
            writer.WriteNumberValue(value);
        }
        else
        {
            writer.WriteStringValue(value.ToString(CultureInfo.InvariantCulture));
        }
    }
}
