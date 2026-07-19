using System.Globalization;

namespace DataVo.Core;

/// <summary>
/// Represents a calendar date as the number of days since January 1, 0001.
/// </summary>
public readonly partial struct DataVoDate :
    IEquatable<DataVoDate>, IComparable<DataVoDate>, IComparable, IFormattable
{
    private const int MinDayNumber = 0;
    private const int MaxDayNumber = 3652058;

    private readonly int _dayNumber;

    private DataVoDate(int dayNumber) => _dayNumber = dayNumber;

    /// <summary>Gets the number of days since January 1, 0001.</summary>
    public int DayNumber => _dayNumber;

    /// <summary>Creates a date from its zero-based day number.</summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="dayNumber"/> is outside the range 0 through 3652058.
    /// </exception>
    public static DataVoDate FromDayNumber(int dayNumber)
    {
        if (dayNumber < MinDayNumber || dayNumber > MaxDayNumber)
        {
            throw new ArgumentOutOfRangeException(nameof(dayNumber));
        }

        return new DataVoDate(dayNumber);
    }

    /// <summary>Creates a date from the date component of a <see cref="DateTime"/> value.</summary>
    public static DataVoDate FromDateTime(DateTime value) =>
        FromDayNumber((int)(value.Ticks / TimeSpan.TicksPerDay));

    /// <summary>Returns the date as a <see cref="DateTime"/> at midnight.</summary>
    public DateTime ToDateTime() =>
        new((long)_dayNumber * TimeSpan.TicksPerDay, DateTimeKind.Unspecified);

    /// <summary>Parses a date using the current culture.</summary>
    public static DataVoDate Parse(string input) =>
        FromDateTime(DateTime.Parse(input, CultureInfo.CurrentCulture, DateTimeStyles.None));

    /// <summary>Attempts to parse a date using the current culture.</summary>
    public static bool TryParse(string? input, out DataVoDate result) =>
        TryParse(input, CultureInfo.CurrentCulture, DateTimeStyles.None, out result);

    /// <summary>Attempts to parse a date using the supplied provider and styles.</summary>
    public static bool TryParse(
        string? input,
        IFormatProvider? provider,
        DateTimeStyles styles,
        out DataVoDate result)
    {
        if (DateTime.TryParse(input, provider, styles, out DateTime value))
        {
            result = FromDateTime(value);
            return true;
        }

        result = default;
        return false;
    }

    /// <inheritdoc />
    public int CompareTo(DataVoDate other) => _dayNumber.CompareTo(other._dayNumber);

    /// <inheritdoc />
    public int CompareTo(object? obj)
    {
        if (obj is null)
        {
            return 1;
        }

        if (obj is DataVoDate other)
        {
            return CompareTo(other);
        }

        throw new ArgumentException("Object must be of type DataVoDate.", nameof(obj));
    }

    /// <inheritdoc />
    public bool Equals(DataVoDate other) => _dayNumber == other._dayNumber;

    /// <inheritdoc />
    public override bool Equals(object? obj) => obj is DataVoDate other && Equals(other);

    /// <inheritdoc />
    public override int GetHashCode() => _dayNumber.GetHashCode();

    /// <summary>Formats the date as invariant <c>yyyy-MM-dd</c>.</summary>
    public override string ToString() =>
        ToDateTime().ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

    /// <inheritdoc />
    public string ToString(string? format, IFormatProvider? formatProvider)
    {
        if (string.IsNullOrEmpty(format)
            || string.Equals(format, "O", StringComparison.Ordinal)
            || string.Equals(format, "yyyy-MM-dd", StringComparison.Ordinal))
        {
            return ToString();
        }

        return ToDateTime().ToString(format, formatProvider);
    }

    /// <summary>Returns whether two dates have the same day number.</summary>
    public static bool operator ==(DataVoDate left, DataVoDate right) => left.Equals(right);

    /// <summary>Returns whether two dates have different day numbers.</summary>
    public static bool operator !=(DataVoDate left, DataVoDate right) => !left.Equals(right);

    /// <summary>Returns whether the left date precedes the right date.</summary>
    public static bool operator <(DataVoDate left, DataVoDate right) => left.CompareTo(right) < 0;

    /// <summary>Returns whether the left date precedes or equals the right date.</summary>
    public static bool operator <=(DataVoDate left, DataVoDate right) => left.CompareTo(right) <= 0;

    /// <summary>Returns whether the left date follows the right date.</summary>
    public static bool operator >(DataVoDate left, DataVoDate right) => left.CompareTo(right) > 0;

    /// <summary>Returns whether the left date follows or equals the right date.</summary>
    public static bool operator >=(DataVoDate left, DataVoDate right) => left.CompareTo(right) >= 0;
}
