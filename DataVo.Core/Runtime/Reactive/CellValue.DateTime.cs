namespace DataVo.Core.Runtime.Reactive;

public readonly partial struct CellValue
{
    /// <summary>
    /// Creates a DATETIME cell from <paramref name="value"/>, stored inline as UTC ticks (no boxing). A local time is
    /// converted to UTC; an unspecified one is taken as UTC.
    /// </summary>
    public static CellValue From(DateTime value) => FromUtcTicks(DataVoDateTime.ToUtcTicks(value));

    /// <summary>Creates a DATETIME cell holding the instant of <paramref name="value"/> (stored as UTC ticks).</summary>
    public static CellValue From(DateTimeOffset value) => FromUtcTicks(value.UtcTicks);

    /// <summary>Creates a DATETIME cell from UTC ticks.</summary>
    public static CellValue FromUtcTicks(long utcTicks)
    {
        if ((ulong)utcTicks > (ulong)DateTime.MaxValue.Ticks)
        {
            throw new ArgumentOutOfRangeException(nameof(utcTicks));
        }

        return new CellValue(CellType.DateTime, utcTicks, 0m, null);
    }

    /// <summary>Reads a DATETIME cell as a <see cref="DateTime"/> of kind <see cref="DateTimeKind.Utc"/>.</summary>
    public DateTime AsDateTime() =>
        _type == CellType.DateTime ? DataVoDateTime.FromUtcTicks(_numeric) : throw Mismatch(CellType.DateTime);

    /// <summary>Reads a DATETIME cell as a <see cref="DateTimeOffset"/> at offset zero.</summary>
    public DateTimeOffset AsDateTimeOffset() =>
        _type == CellType.DateTime ? DataVoDateTime.ToOffset(_numeric) : throw Mismatch(CellType.DateTime);

    /// <summary>Reads a DATETIME cell's UTC ticks.</summary>
    public long AsUtcTicks() =>
        _type == CellType.DateTime ? _numeric : throw Mismatch(CellType.DateTime);
}
