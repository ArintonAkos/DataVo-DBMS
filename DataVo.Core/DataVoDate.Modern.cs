#if NET10_0_OR_GREATER
namespace DataVo.Core;

public readonly partial struct DataVoDate
{
    /// <summary>Creates a date from a <see cref="DateOnly"/> value.</summary>
    public static DataVoDate FromDateOnly(DateOnly value) => FromDayNumber(value.DayNumber);

    /// <summary>Returns the date as a <see cref="DateOnly"/> value.</summary>
    public DateOnly ToDateOnly() => DateOnly.FromDayNumber(_dayNumber);
}
#endif
