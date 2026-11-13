using System.Buffers.Binary;
using StorageKind = DataVo.Core.StorageEngine.Serialization.RowSerializer.StorageColumnType;

namespace DataVo.Core.Indexing.Columnar;

/// <summary>
/// Turns one cell of a stored row into the value a columnar run holds for it. Shared by the in-memory
/// <see cref="ColumnarIndex"/> and the columnar segment of an SSTable, so both hold the same bits for a row.
/// </summary>
/// <remarks>
/// A value travels as a 64-bit payload: an INT, a DATE's day number or a BIT (0 or 1) as that integer, a BIGINT
/// as itself, and a FLOAT or DOUBLE as its IEEE bits (a FLOAT's in the low half).
/// </remarks>
internal static class ColumnarCells
{
    /// <summary>The bytes one value of a type takes in a run; a BIT takes one bit and has no width.</summary>
    public static int WidthOf(StorageKind kind) => kind is StorageKind.BigInt or StorageKind.Double ? sizeof(long) : sizeof(int);

    /// <summary>Reads the cell whose value starts at <paramref name="offset"/> of a stored row.</summary>
    public static long ReadBits(StorageKind kind, ReadOnlySpan<byte> stored, int offset) => kind switch
    {
        StorageKind.Int => RowBytes.Int32(stored, offset),
        StorageKind.BigInt => RowBytes.Int64(stored, offset),
        StorageKind.Double => BitConverter.DoubleToInt64Bits(RowBytes.Double(stored, offset)),
        StorageKind.Float => BitConverter.SingleToInt32Bits(RowBytes.Single(stored, offset)),
        StorageKind.Date => RowBytes.DayNumber(stored, offset),
        StorageKind.Bit => RowBytes.Boolean(stored, offset) ? 1 : 0,
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Not a columnar type."),
    };

    /// <summary>
    /// The value of a clustered table's key column for a row key. Only integer keys have one.
    /// </summary>
    /// <returns><see langword="false"/> when the key is not an integer key.</returns>
    public static bool TryKeyBits(StorageKind kind, RowKey key, out long bits)
    {
        if (!key.IsInt64)
        {
            bits = 0;
            return false;
        }

        bits = kind == StorageKind.BigInt ? (long)key : (int)(long)key;
        return true;
    }

    /// <summary>Whether a FLOAT or DOUBLE payload is a NaN; false for every other type.</summary>
    public static bool IsNaN(StorageKind kind, long bits) => kind switch
    {
        StorageKind.Double => double.IsNaN(BitConverter.Int64BitsToDouble(bits)),
        StorageKind.Float => float.IsNaN(BitConverter.Int32BitsToSingle((int)bits)),
        _ => false,
    };

    /// <summary>Writes a payload as value <paramref name="at"/> of a little-endian run of a type that has a width.</summary>
    public static void Store(StorageKind kind, Span<byte> run, int at, long bits)
    {
        if (WidthOf(kind) == sizeof(long))
        {
            BinaryPrimitives.WriteInt64LittleEndian(run[(at * sizeof(long))..], bits);
        }
        else
        {
            BinaryPrimitives.WriteInt32LittleEndian(run[(at * sizeof(int))..], (int)bits);
        }
    }

    /// <summary>
    /// A payload as a chunk's minimum or maximum is stored: a FLOAT widened to the bits of a double, so one
    /// reader decodes both floating types; every other type unchanged.
    /// </summary>
    public static long ZoneBits(StorageKind kind, long bits) =>
        kind == StorageKind.Float ? BitConverter.DoubleToInt64Bits(BitConverter.Int32BitsToSingle((int)bits)) : bits;

    /// <summary>
    /// Orders two values in <see cref="ZoneBits"/> form by the column's type: floating types as numbers, so
    /// negative values order below positive ones, the rest as integers.
    /// </summary>
    public static int CompareZone(StorageKind kind, long left, long right) =>
        kind is StorageKind.Float or StorageKind.Double
            ? BitConverter.Int64BitsToDouble(left).CompareTo(BitConverter.Int64BitsToDouble(right))
            : left.CompareTo(right);
}
