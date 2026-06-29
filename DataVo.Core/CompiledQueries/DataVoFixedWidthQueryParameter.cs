namespace DataVo.Core.CompiledQueries;

/// <summary>Primitive kinds supported by the fixed-width compiled-update hot path.</summary>
public enum DataVoFixedWidthValueType : byte
{
    /// <summary>A Boolean value.</summary>
    Boolean,
    /// <summary>A 32-bit signed integer.</summary>
    Int32,
    /// <summary>A 64-bit signed integer.</summary>
    Int64,
    /// <summary>A 64-bit floating-point value.</summary>
    Double,
}

/// <summary>An unmanaged primitive value carrier for allocation-free fixed-width compiled updates.</summary>
/// <param name="Type">The primitive kind stored in <paramref name="Bits"/>.</param>
/// <param name="Bits">The raw numeric bits for the value.</param>
public readonly record struct DataVoFixedWidthValue(DataVoFixedWidthValueType Type, long Bits)
{
    /// <summary>Creates a Boolean fixed-width value.</summary>
    public static DataVoFixedWidthValue From(bool value) =>
        new(DataVoFixedWidthValueType.Boolean, value ? 1L : 0L);

    /// <summary>Creates a 32-bit integer fixed-width value.</summary>
    public static DataVoFixedWidthValue From(int value) =>
        new(DataVoFixedWidthValueType.Int32, value);

    /// <summary>Creates a 64-bit integer fixed-width value.</summary>
    public static DataVoFixedWidthValue From(long value) =>
        new(DataVoFixedWidthValueType.Int64, value);

    /// <summary>Creates a double fixed-width value.</summary>
    public static DataVoFixedWidthValue From(double value) =>
        new(DataVoFixedWidthValueType.Double, BitConverter.DoubleToInt64Bits(value));

    /// <summary>Reads the value as a Boolean.</summary>
    public bool AsBoolean() => Type == DataVoFixedWidthValueType.Boolean
        ? Bits != 0L
        : throw new InvalidOperationException($"Value holds {Type}, not Boolean.");

    /// <summary>Reads the value as a 32-bit integer.</summary>
    public int AsInt32() => Type == DataVoFixedWidthValueType.Int32
        ? (int)Bits
        : throw new InvalidOperationException($"Value holds {Type}, not Int32.");

    /// <summary>Reads the value as a 64-bit integer.</summary>
    public long AsInt64() => Type switch
    {
        DataVoFixedWidthValueType.Int32 => (int)Bits,
        DataVoFixedWidthValueType.Int64 => Bits,
        _ => throw new InvalidOperationException($"Value holds {Type}, not Int64."),
    };

    /// <summary>Reads the value as a double.</summary>
    public double AsDouble() => Type == DataVoFixedWidthValueType.Double
        ? BitConverter.Int64BitsToDouble(Bits)
        : throw new InvalidOperationException($"Value holds {Type}, not Double.");

    /// <summary>Boxes the value for fallback paths.</summary>
    public object ToObject() => Type switch
    {
        DataVoFixedWidthValueType.Boolean => AsBoolean(),
        DataVoFixedWidthValueType.Int32 => AsInt32(),
        DataVoFixedWidthValueType.Int64 => AsInt64(),
        DataVoFixedWidthValueType.Double => AsDouble(),
        _ => throw new InvalidOperationException($"Unsupported fixed-width value type {Type}."),
    };

    /// <summary>Attempts to convert a boxed primitive into an allocation-free fixed-width value.</summary>
    public static bool TryFromObject(object value, out DataVoFixedWidthValue fixedValue)
    {
        switch (value)
        {
            case bool b:
                fixedValue = From(b);
                return true;
            case int i:
                fixedValue = From(i);
                return true;
            case long l:
                fixedValue = From(l);
                return true;
            case double d:
                fixedValue = From(d);
                return true;
            default:
                fixedValue = default;
                return false;
        }
    }
}

/// <summary>
/// A named compiled-query parameter for fixed-width hot paths. Values are carried as <see cref="DataVoFixedWidthValue"/>
/// so primitive update loops avoid boxing.
/// </summary>
public readonly record struct DataVoFixedWidthQueryParameter(string Name, DataVoFixedWidthValue Value);

/// <summary>
/// A fixed-width primary-key update batch entry carrying the benchmark hot-path shape without per-row arrays.
/// </summary>
public readonly record struct DataVoFixedWidthUpdateBatchEntry(
    DataVoFixedWidthValue PrimaryKey,
    DataVoFixedWidthValue Value0,
    DataVoFixedWidthValue Value1);
