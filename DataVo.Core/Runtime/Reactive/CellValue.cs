using System.Buffers.Binary;

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
    /// <summary>A <see cref="DateOnly"/> (the SQL DATE type), stored inline as its day number.</summary>
    Date = 7,
    /// <summary>A dense <see cref="float"/>[] (the SQL VECTOR type); the cell owns a clone of the array.</summary>
    Vector = 8,
    /// <summary>A 128-bit <see cref="Guid"/>.</summary>
    Guid = 9,
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
    private readonly decimal _decimal;  // inline decimal, or second half of Guid bytes when Type == Guid
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

    /// <summary>Creates a <see cref="DateOnly"/> cell (stored inline as its day number; no boxing).</summary>
    public static CellValue From(DateOnly value) => new(CellType.Date, value.DayNumber, 0m, null);

    /// <summary>Creates a <see cref="Guid"/> cell (stored inline as two 64-bit halves; no boxing).</summary>
    public static CellValue From(Guid value)
    {
        Span<byte> bytes = stackalloc byte[16];
        value.TryWriteBytes(bytes);
        long low = BinaryPrimitives.ReadInt64LittleEndian(bytes[..8]);
        long high = BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(8, 8));
        return new CellValue(CellType.Guid, low, DecimalFromInt64Bits(high), null);
    }

    /// <summary>Creates a VECTOR (<see cref="float"/>[]) cell; the array is <b>cloned</b> so the cell owns
    /// it (callers cannot mutate stored state). <c>null</c> maps to <see cref="Null"/>.</summary>
    public static CellValue From(float[]? value) =>
        value is null ? Null : new(CellType.Vector, 0L, 0m, (float[])value.Clone());

    internal static CellValue FromVectorOwned(float[]? value) =>
        value is null ? Null : new(CellType.Vector, 0L, 0m, value);

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
        DateOnly d => From(d),
        Guid g => From(g),
        float[] v => From(v),
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

    /// <summary>Reads the cell as a <see cref="DateOnly"/>.</summary>
    public DateOnly AsDate() =>
        _type == CellType.Date ? DateOnly.FromDayNumber((int)_numeric) : throw Mismatch(CellType.Date);

    /// <summary>Reads the cell as a <see cref="Guid"/>.</summary>
    public Guid AsGuid()
    {
        if (_type != CellType.Guid)
        {
            throw Mismatch(CellType.Guid);
        }

        Span<byte> bytes = stackalloc byte[16];
        BinaryPrimitives.WriteInt64LittleEndian(bytes[..8], _numeric);
        BinaryPrimitives.WriteInt64LittleEndian(bytes.Slice(8, 8), Int64BitsFromDecimal(_decimal));
        return new Guid(bytes);
    }

    /// <summary>Reads the cell as a VECTOR (<see cref="float"/>[]), returning a defensive <b>clone</b> so
    /// the stored array can never be mutated through the result.</summary>
    public float[] AsVector() =>
        _type == CellType.Vector ? (float[])((float[])_reference!).Clone() : throw Mismatch(CellType.Vector);

    internal int VectorLength =>
        _type == CellType.Vector ? ((float[])_reference!).Length : throw Mismatch(CellType.Vector);

    internal ReadOnlySpan<float> AsVectorReadOnlySpan() =>
        _type == CellType.Vector ? ((float[])_reference!).AsSpan() : throw Mismatch(CellType.Vector);

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
        CellType.Date => DateOnly.FromDayNumber((int)_numeric),
        CellType.Vector => ((float[])_reference!).Clone(),
        CellType.Guid => AsGuid(),
        _ => null,
    };

    private InvalidOperationException Mismatch(CellType expected) =>
        new($"CellValue holds {_type}, not {expected}.");

    private static decimal DecimalFromInt64Bits(long value)
    {
        ulong bits = unchecked((ulong)value);
        return new decimal(
            unchecked((int)(bits & 0xFFFFFFFFUL)),
            unchecked((int)(bits >> 32)),
            0,
            false,
            0);
    }

    private static long Int64BitsFromDecimal(decimal value)
    {
        Span<int> bits = stackalloc int[4];
        decimal.GetBits(value, bits);
        ulong raw = (uint)bits[0] | ((ulong)(uint)bits[1] << 32);
        return unchecked((long)raw);
    }
}
