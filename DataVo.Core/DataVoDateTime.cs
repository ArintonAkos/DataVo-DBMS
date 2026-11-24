using System.Globalization;

namespace DataVo.Core;

/// <summary>
/// The SQL <c>DATETIME</c> / <c>TIMESTAMP</c> type: a point in time, stored as 8-byte UTC ticks.
/// </summary>
/// <remarks>
/// <para>
/// A value is stored as <see cref="DateTime.Ticks"/> of its UTC instant (a <see cref="long"/>), so packed rows,
/// index keys and columnar scans compare it as a plain 64-bit integer. Reads return a <see cref="DateTime"/> of
/// kind <see cref="DateTimeKind.Utc"/>.
/// </para>
/// <para>
/// Conversions: a <see cref="DateTime"/> of kind <see cref="DateTimeKind.Local"/> is converted to UTC; one of kind
/// <see cref="DateTimeKind.Utc"/> or <see cref="DateTimeKind.Unspecified"/> is taken as UTC as it is. A
/// <see cref="DateTimeOffset"/> keeps its instant (<see cref="DateTimeOffset.UtcTicks"/>). Text is ISO 8601 in the
/// invariant culture (<c>2026-11-24</c>, <c>2026-11-24 10:15:00</c>, <c>2026-11-24T10:15:00.1234567Z</c>,
/// <c>2026-11-24T12:15:00+02:00</c>); text without an offset is UTC.
/// </para>
/// </remarks>
internal static class DataVoDateTime
{
    /// <summary>The type name stored in the catalog.</summary>
    public const string SqlTypeName = "DATETIME";

    /// <summary>The canonical text form: fixed width, so ordinal order is chronological order.</summary>
    public const string CanonicalFormat = "yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'";

    private static readonly string[] Formats =
    [
        "yyyy-MM-dd'T'HH:mm:ss.FFFFFFFK",
        "yyyy-MM-dd HH:mm:ss.FFFFFFFK",
        "yyyy-MM-dd'T'HH:mmK",
        "yyyy-MM-dd HH:mmK",
        "yyyy-MM-dd",
    ];

    /// <summary>Whether <paramref name="columnType"/> names the DATETIME type.</summary>
    public static bool IsDateTimeType(string? columnType) =>
        string.Equals(columnType, SqlTypeName, StringComparison.OrdinalIgnoreCase);

    /// <summary>The UTC ticks of <paramref name="value"/> (local times converted, unspecified taken as UTC).</summary>
    public static long ToUtcTicks(DateTime value) =>
        value.Kind == DateTimeKind.Local ? value.ToUniversalTime().Ticks : value.Ticks;

    /// <summary>The UTC ticks of <paramref name="value"/>.</summary>
    public static long ToUtcTicks(DateTimeOffset value) => value.UtcTicks;

    /// <summary>The UTC <see cref="DateTime"/> of stored ticks.</summary>
    public static DateTime FromUtcTicks(long utcTicks) => new(utcTicks, DateTimeKind.Utc);

    /// <summary>A <see cref="DateTimeOffset"/> at offset zero for stored ticks.</summary>
    public static DateTimeOffset ToOffset(long utcTicks) => new(utcTicks, TimeSpan.Zero);

    /// <summary>Normalizes a value to a UTC <see cref="DateTime"/>.</summary>
    public static DateTime Normalize(DateTime value) => FromUtcTicks(ToUtcTicks(value));

    /// <summary>The canonical text of stored ticks (round-trips through <see cref="TryParse"/>).</summary>
    public static string Format(long utcTicks) =>
        FromUtcTicks(utcTicks).ToString(CanonicalFormat, CultureInfo.InvariantCulture);

    /// <summary>The canonical text of <paramref name="value"/> after normalization to UTC.</summary>
    public static string Format(DateTime value) => Format(ToUtcTicks(value));

    /// <summary>Parses ISO 8601 text (optionally single-quoted); text without an offset is UTC.</summary>
    public static bool TryParse(string? text, out long utcTicks)
    {
        utcTicks = 0;
        if (text is null)
        {
            return false;
        }

        ReadOnlySpan<char> span = text.AsSpan().Trim();
        if (span.Length >= 2 && span[0] == '\'' && span[^1] == '\'')
        {
            span = span[1..^1].Trim();
        }

        if (span.IsEmpty || !char.IsDigit(span[0]))
        {
            return false;
        }

        if (DateTimeOffset.TryParseExact(
                span.ToString(),
                Formats,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AllowWhiteSpaces,
                out DateTimeOffset parsed))
        {
            utcTicks = parsed.UtcTicks;
            return true;
        }

        return false;
    }

    /// <summary>
    /// Converts a CLR value bound to a DATETIME column (a <see cref="DateTime"/>, <see cref="DateTimeOffset"/>,
    /// <see cref="DataVoDate"/> at midnight UTC, or ISO 8601 text) to UTC ticks.
    /// </summary>
    public static bool TryConvert(object? value, out long utcTicks)
    {
        switch (value)
        {
            case DateTime dateTime:
                utcTicks = ToUtcTicks(dateTime);
                return true;
            case DateTimeOffset offset:
                utcTicks = offset.UtcTicks;
                return true;
            case DataVoDate date:
                utcTicks = (long)date.DayNumber * TimeSpan.TicksPerDay;
                return true;
            case string text:
                return TryParse(text, out utcTicks);
            default:
                utcTicks = 0;
                return false;
        }
    }
}
