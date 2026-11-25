using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using DataVo.Core.Constants;
using Microsoft.Extensions.AI;

namespace DataVo.VectorData;

/// <summary>
/// Converts between record property values and DataVo column values: the SQL type a CLR type is stored as, the
/// value bound as a statement parameter, and the property value rebuilt from a result cell.
/// </summary>
internal static class DataVoValueMapper
{
    /// <summary>The DataVo SQL type a data property of <paramref name="type"/> is stored as; null when it has none.</summary>
    public static string? SqlTypeOf(Type type)
    {
        Type t = Nullable.GetUnderlyingType(type) ?? type;
        if (t == typeof(string))
        {
            return "VARCHAR";
        }

        if (t == typeof(int) || t == typeof(short) || t == typeof(byte) || t == typeof(sbyte) || t == typeof(ushort))
        {
            return "INT";
        }

        if (t == typeof(long) || t == typeof(uint))
        {
            return "BIGINT";
        }

        if (t == typeof(float))
        {
            return "FLOAT";
        }

        if (t == typeof(double) || t == typeof(decimal))
        {
            return "DOUBLE";
        }

        if (t == typeof(bool))
        {
            return "BIT";
        }

        if (t == typeof(Guid))
        {
            return "GUID";
        }

        if (t == typeof(DateTime) || t == typeof(DateTimeOffset))
        {
            return "DATETIME";
        }

        if (t == typeof(DateOnly))
        {
            return "DATE";
        }

        // String lists are kept as JSON text: stored and returned, but not filterable.
        return t == typeof(string[]) || t == typeof(List<string>) ? "VARCHAR" : null;
    }

    /// <summary>Whether values of a data property can be compared in a filter (string lists, kept as JSON, cannot).</summary>
    public static bool IsFilterable(Type type) => type != typeof(string[]) && type != typeof(List<string>);

    /// <summary>The value bound for a property's column (vectors as floats, string lists as JSON).</summary>
    public static object? ToColumn(object? value)
    {
        switch (value)
        {
            case null:
                return null;
            case Embedding<float> embedding:
                return embedding.Vector;
            case string[] strings:
                return JsonSerializer.Serialize(strings, DataVoVectorDataJsonContext.Default.StringArray);
            case List<string> list:
                return JsonSerializer.Serialize(list, DataVoVectorDataJsonContext.Default.ListString);
            case uint u:
                return (long)u;
            case ushort us:
                return (int)us;
            case sbyte sb:
                return (int)sb;
            case DateTime dateTime:
                return dateTime.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(dateTime, DateTimeKind.Utc) : dateTime;
            default:
                return value;
        }
    }

    /// <summary>Rebuilds a property value of <paramref name="type"/> from a result cell.</summary>
    public static object? FromColumn(object? cell, Type type)
    {
        if (cell is null or DBNull)
        {
            return null;
        }

        Type t = Nullable.GetUnderlyingType(type) ?? type;

        // DataVo stores a DATETIME as UTC ticks and returns it with an unspecified kind: mark it UTC.
        if (t == typeof(DateTime))
        {
            return AsUtc(cell);
        }

        if (t.IsInstanceOfType(cell) && t != typeof(object))
        {
            return cell;
        }

        if (t == typeof(ReadOnlyMemory<float>))
        {
            return new ReadOnlyMemory<float>(ToFloats(cell));
        }

        if (t == typeof(float[]))
        {
            return ToFloats(cell);
        }

        if (t == typeof(Embedding<float>))
        {
            return new Embedding<float>(ToFloats(cell));
        }

        if (t == typeof(Guid))
        {
            return cell is string text ? Guid.Parse(text) : (Guid)cell;
        }

        if (t == typeof(DateTimeOffset))
        {
            return new DateTimeOffset(AsUtc(cell));
        }

        if (t == typeof(DateOnly))
        {
            return cell switch
            {
                DateTime dateTime => DateOnly.FromDateTime(dateTime),
                string text => DateOnly.Parse(text, CultureInfo.InvariantCulture),
                _ => (DateOnly)cell,
            };
        }

        if (t == typeof(bool))
        {
            return cell is bool flag ? flag : Convert.ToInt64(cell, CultureInfo.InvariantCulture) != 0;
        }

        if (t == typeof(string[]))
        {
            return JsonSerializer.Deserialize((string)cell, DataVoVectorDataJsonContext.Default.StringArray);
        }

        if (t == typeof(List<string>))
        {
            return JsonSerializer.Deserialize((string)cell, DataVoVectorDataJsonContext.Default.ListString);
        }

        if (t == typeof(string))
        {
            return Convert.ToString(cell, CultureInfo.InvariantCulture);
        }

        if (t == typeof(object))
        {
            return cell;
        }

        return Convert.ChangeType(cell, t, CultureInfo.InvariantCulture);
    }

    private static float[] ToFloats(object cell) => cell switch
    {
        float[] floats => floats,
        ReadOnlyMemory<float> memory => memory.ToArray(),
        string text => ParseVector(text),
        _ => throw new InvalidOperationException($"A vector column held a {cell.GetType().Name}."),
    };

    /// <summary>Parses the text form of a vector, <c>[1, 0.5, -2]</c>.</summary>
    private static float[] ParseVector(string text)
    {
        ReadOnlySpan<char> body = text.AsSpan().Trim();
        if (body.Length < 2 || body[0] != '[' || body[^1] != ']')
        {
            throw new InvalidOperationException("A vector column held text that is not a vector.");
        }

        string[] parts = body[1..^1].ToString().Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var values = new float[parts.Length];
        for (int i = 0; i < parts.Length; i++)
        {
            values[i] = float.Parse(parts[i], NumberStyles.Float, CultureInfo.InvariantCulture);
        }

        return values;
    }

    private static DateTime AsUtc(object cell) => cell switch
    {
        DateTime dateTime => dateTime.Kind == DateTimeKind.Utc ? dateTime : DateTime.SpecifyKind(dateTime, DateTimeKind.Utc),
        DateTimeOffset offset => offset.UtcDateTime,
        string text => DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal),
        _ => throw new InvalidOperationException($"A DATETIME column held a {cell.GetType().Name}."),
    };
}

/// <summary>Names of tables and columns: DataVo SQL identifiers have no quoting, so names are checked up front.</summary>
internal static class DataVoSqlNames
{
    /// <summary>Throws unless <paramref name="name"/> is a plain identifier (a letter or underscore, then letters, digits, underscores).</summary>
    public static void ValidateIdentifier(string name, string what)
    {
        if (string.IsNullOrEmpty(name) || !(char.IsAsciiLetter(name[0]) || name[0] == '_') || !name.All(c => char.IsAsciiLetterOrDigit(c) || c == '_'))
        {
            throw new ArgumentException($"'{name}' cannot be a DataVo {what} name: use letters, digits and underscores, starting with a letter or underscore.");
        }
    }

    /// <summary>Throws unless <paramref name="name"/> can name a collection (a table): a plain identifier that is not a reserved word.</summary>
    public static void ValidateCollectionName(string name)
    {
        ValidateIdentifier(name, "collection");
        if (SqlKeywords.All.Contains(name.ToUpperInvariant()))
        {
            throw new ArgumentException($"'{name}' is a reserved DataVo SQL word and cannot name a collection.");
        }
    }
}

/// <summary>JSON metadata for string-list data properties (trim- and AOT-safe).</summary>
[JsonSerializable(typeof(string[]))]
[JsonSerializable(typeof(List<string>))]
internal sealed partial class DataVoVectorDataJsonContext : JsonSerializerContext;
