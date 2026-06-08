namespace DataVo.Core.Runtime.Reactive;

/// <summary>Discriminator for the scalar held by a <see cref="CellValue"/>.</summary>
public enum CellType : byte
{
    /// <summary>SQL NULL (also the canonical representation of a null string).</summary>
    Null = 0,
    /// <summary>A <see cref="bool"/>.</summary>
    Boolean = 1,
    /// <summary>A 32-bit signed integer.</summary>
    Int32 = 2,
    /// <summary>A 64-bit signed integer.</summary>
    Int64 = 3,
    /// <summary>A 64-bit IEEE float.</summary>
    Double = 4,
    /// <summary>A 128-bit <see cref="decimal"/> (stored inline; never boxed).</summary>
    Decimal = 5,
    /// <summary>A <see cref="string"/>.</summary>
    String = 6,
}

/// <summary>
/// A compact, typed cell value that avoids boxing for the common DataVo scalar types
/// (<c>null</c>, <see cref="bool"/>, <see cref="int"/>, <see cref="long"/>, <see cref="double"/>,
/// <see cref="decimal"/>, <see cref="string"/>). Decimal is stored inline so financial workloads
/// stay allocation-free. Replaces <c>object?</c> as the reactive engine's cell currency.
/// </summary>
public readonly struct CellValue
{
    private readonly CellType _type;
    private readonly long _numeric;     // bool/int/long/double bits (reinterpreted)
    private readonly decimal _decimal;  // inline; never boxed
    private readonly object? _reference; // string today; reference types later

    private CellValue(CellType type, long numeric, decimal dec, object? reference)
    {
        _type = type;
        _numeric = numeric;
        _decimal = dec;
        _reference = reference;
    }

    /// <summary>The canonical NULL cell.</summary>
    public static readonly CellValue Null = new(CellType.Null, 0L, 0m, null);

    /// <summary>Creates a <see cref="bool"/> cell.</summary>
    public static CellValue From(bool value) => new(CellType.Boolean, value ? 1L : 0L, 0m, null);

    /// <summary>Creates an <see cref="int"/> cell.</summary>
    public static CellValue From(int value) => new(CellType.Int32, value, 0m, null);

    /// <summary>Creates a <see cref="long"/> cell.</summary>
    public static CellValue From(long value) => new(CellType.Int64, value, 0m, null);

    /// <summary>Creates a <see cref="double"/> cell.</summary>
    public static CellValue From(double value) =>
        new(CellType.Double, BitConverter.DoubleToInt64Bits(value), 0m, null);

    /// <summary>Creates a <see cref="decimal"/> cell (stored inline, no boxing).</summary>
    public static CellValue From(decimal value) => new(CellType.Decimal, 0L, value, null);

    /// <summary>Creates a <see cref="string"/> cell; <c>null</c> maps to <see cref="Null"/>.</summary>
    public static CellValue From(string? value) =>
        value is null ? Null : new(CellType.String, 0L, 0m, value);

    /// <summary>
    /// Compatibility-only: builds a cell from a boxed value. NOT for the hot path — operators must
    /// construct cells from typed values.
    /// </summary>
    public static CellValue From(object? value) => value switch
    {
        null => Null,
        bool b => From(b),
        int i => From(i),
        long l => From(l),
        double d => From(d),
        decimal m => From(m),
        string s => From(s),
        _ => throw new NotSupportedException($"Unsupported cell value type '{value.GetType()}'."),
    };

    /// <summary>The scalar type held by this cell.</summary>
    public CellType Type => _type;

    /// <summary>Whether this cell is NULL.</summary>
    public bool IsNull => _type == CellType.Null;

    /// <summary>Reads the cell as a <see cref="bool"/>.</summary>
    public bool AsBoolean() =>
        _type == CellType.Boolean ? _numeric != 0L : throw Mismatch(CellType.Boolean);

    /// <summary>Reads the cell as an <see cref="int"/>.</summary>
    public int AsInt32() =>
        _type == CellType.Int32 ? (int)_numeric : throw Mismatch(CellType.Int32);

    /// <summary>Reads the cell as a <see cref="long"/>.</summary>
    public long AsInt64() =>
        _type == CellType.Int64 ? _numeric : throw Mismatch(CellType.Int64);

    /// <summary>Reads the cell as a <see cref="double"/>.</summary>
    public double AsDouble() =>
        _type == CellType.Double ? BitConverter.Int64BitsToDouble(_numeric) : throw Mismatch(CellType.Double);

    /// <summary>Reads the cell as a <see cref="decimal"/>.</summary>
    public decimal AsDecimal() =>
        _type == CellType.Decimal ? _decimal : throw Mismatch(CellType.Decimal);

    /// <summary>Reads the cell as a <see cref="string"/>; NULL cells return <c>null</c>.</summary>
    public string? AsString() => _type switch
    {
        CellType.String => (string?)_reference,
        CellType.Null => null,
        _ => throw Mismatch(CellType.String),
    };

    /// <summary>
    /// Compatibility-only: boxes the cell into <c>object?</c> for materialization. NOT for the hot path.
    /// </summary>
    public object? ToObject() => _type switch
    {
        CellType.Null => null,
        CellType.Boolean => _numeric != 0L,
        CellType.Int32 => (int)_numeric,
        CellType.Int64 => _numeric,
        CellType.Double => BitConverter.Int64BitsToDouble(_numeric),
        CellType.Decimal => _decimal,
        CellType.String => _reference,
        _ => null,
    };

    private InvalidOperationException Mismatch(CellType expected) =>
        new($"CellValue holds {_type}, not {expected}.");
}
