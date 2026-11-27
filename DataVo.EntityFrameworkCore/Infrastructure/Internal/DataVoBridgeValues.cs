using System.Globalization;

namespace DataVo.EntityFrameworkCore.Infrastructure.Internal;

/// <summary>
/// Converts CLR values into the forms DataVo stores faithfully, for parameters and DDL literals.
/// </summary>
internal static class DataVoBridgeValues
{
    private const string UtcInstantFormat = "yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'";

    /// <summary>
    /// Converts a CLR value into a value the DataVo parameter binder stores without loss: enums as their
    /// number, narrow and unsigned integers widened, <see cref="char"/> as text and
    /// <see cref="TimeOnly"/> as a <c>DATETIME</c> instant on <c>0001-01-01</c>.
    /// </summary>
    internal static object? ToDataVoValue(object? value) => value switch
    {
        null or DBNull => null,
        Enum enumValue => ToEnumNumber(enumValue),
        char charValue => charValue.ToString(),
        sbyte sbyteValue => (int)sbyteValue,
        ushort ushortValue => (int)ushortValue,
        uint uintValue => (long)uintValue,
        ulong ulongValue => checked((long)ulongValue),
        TimeOnly timeValue => ToDateTime(timeValue),
        _ => value,
    };

    /// <summary>Returns the <c>DATETIME</c> instant used to store a <see cref="TimeOnly"/>.</summary>
    internal static DateTime ToDateTime(TimeOnly value) => DateOnly.MinValue.ToDateTime(value, DateTimeKind.Utc);

    /// <summary>Returns whether an enum type needs a 64-bit column.</summary>
    internal static bool IsWideEnum(Type enumType)
    {
        Type underlying = Enum.GetUnderlyingType(enumType);
        return underlying == typeof(long) || underlying == typeof(ulong) || underlying == typeof(uint);
    }

    /// <summary>
    /// Formats a value as a DataVo SQL literal for a <c>DEFAULT</c> clause. Instants are written as
    /// ISO 8601 UTC text so <c>DATETIME</c> defaults keep their full time of day.
    /// </summary>
    internal static string FormatLiteral(object? value)
    {
        return ToDataVoValue(value) switch
        {
            null => "NULL",
            bool booleanValue => booleanValue ? "true" : "false",
            string stringValue => Quote(stringValue),
            DateOnly dateOnlyValue => Quote(dateOnlyValue.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)),
            DateTime dateTimeValue => Quote(ToUtc(dateTimeValue).ToString(UtcInstantFormat, CultureInfo.InvariantCulture)),
            DateTimeOffset offsetValue => Quote(offsetValue.UtcDateTime.ToString(UtcInstantFormat, CultureInfo.InvariantCulture)),
            Guid guidValue => Quote(guidValue.ToString()),
            float[] vector => Quote(FormatVector(vector)),
            ReadOnlyMemory<float> vector => Quote(FormatVector(vector.ToArray())),
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            var other => Quote(other.ToString() ?? string.Empty),
        };
    }

    private static object ToEnumNumber(Enum value)
    {
        return IsWideEnum(value.GetType())
            ? Convert.ToInt64(value, CultureInfo.InvariantCulture)
            : Convert.ToInt32(value, CultureInfo.InvariantCulture);
    }

    // DataVo reads a DATETIME without an offset as UTC, so only local times need converting.
    private static DateTime ToUtc(DateTime value)
        => value.Kind == DateTimeKind.Local ? value.ToUniversalTime() : value;

    private static string FormatVector(float[] vector)
        => $"[{string.Join(",", vector.Select(static component => component.ToString("R", CultureInfo.InvariantCulture)))}]";

    private static string Quote(string value) => $"'{value.Replace("'", "''")}'";
}
