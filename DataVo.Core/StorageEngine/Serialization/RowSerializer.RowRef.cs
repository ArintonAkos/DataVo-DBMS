using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.Core.StorageEngine.Serialization;

// Span-level helpers behind RowRef: locate every cell of a stored row in one pass, and write typed cells in
// the stored wire format without a stream, so typed backends can hand out the same bytes as serialized ones.
public static partial class RowSerializer
{
    /// <summary>Marks a cell missing from a row written before its column existed; it reads as NULL.</summary>
    internal const int AbsentCell = -1;

    /// <summary>
    /// Records the offset of every cell's null flag in <paramref name="flagOffsets"/>, walking the row once.
    /// Cells past the end of a shorter row get <see cref="AbsentCell"/>. Returns false on a malformed row.
    /// </summary>
    internal static bool TryLocateCells(ReadOnlySpan<byte> row, ReadOnlySpan<StorageColumnType> kinds, Span<int> flagOffsets)
    {
        int offset = 0;
        for (int i = 0; i < kinds.Length; i++)
        {
            if (offset >= row.Length)
            {
                flagOffsets[i] = AbsentCell;
                continue;
            }

            flagOffsets[i] = offset;
            bool isNull = row[offset++] != 0;
            if (isNull)
            {
                continue;
            }

            // Most cells have a fixed width, which is added without a call.
            int width = FixedWidths[(int)kinds[i]];
            if (width != 0)
            {
                offset += width;
                if (offset > row.Length)
                {
                    return false;
                }
            }
            else if (!TryAdvancePastValue(row, kinds[i], ref offset))
            {
                return false;
            }
        }

        return true;
    }

    // The stored width of each StorageColumnType's value, in enum order; 0 for the variable-width kinds.
    private static ReadOnlySpan<byte> FixedWidths => [4, 8, 4, 8, 1, 8, 8, 16, 0, 0];

    /// <summary>The stored width of a value of this kind, or 0 when the width varies from row to row.</summary>
    internal static int FixedWidth(StorageColumnType kind) => FixedWidths[(int)kind];

    private static bool TryAdvancePastValue(ReadOnlySpan<byte> row, StorageColumnType kind, ref int offset)
    {
        switch (kind)
        {
            case StorageColumnType.Int:
            case StorageColumnType.Float:
                offset += sizeof(int);
                break;
            case StorageColumnType.Bit:
                offset += sizeof(bool);
                break;
            case StorageColumnType.BigInt:
            case StorageColumnType.Double:
            case StorageColumnType.Date:
            case StorageColumnType.DateTime:
                offset += sizeof(long);
                break;
            case StorageColumnType.Guid:
                offset += 16;
                break;
            case StorageColumnType.Vector:
                if (offset + sizeof(int) > row.Length)
                {
                    return false;
                }

                int count = BinaryPrimitives.ReadInt32LittleEndian(row.Slice(offset, sizeof(int)));
                if (count < 0 || count > (row.Length - offset - sizeof(int)) / sizeof(float))
                {
                    return false;
                }

                offset += sizeof(int) + (count * sizeof(float));
                break;
            default:
                return TryAdvancePastString(row, ref offset);
        }

        return offset <= row.Length;
    }

    /// <summary>The number of bytes <see cref="WriteCell"/> writes for a cell, null flag included.</summary>
    internal static int MeasureCell(StorageColumnType kind, CellValue cell)
    {
        if (cell.IsNull)
        {
            return 1;
        }

        return 1 + kind switch
        {
            StorageColumnType.Int or StorageColumnType.Float => sizeof(int),
            StorageColumnType.Bit => sizeof(bool),
            StorageColumnType.BigInt or StorageColumnType.Double or StorageColumnType.Date or StorageColumnType.DateTime => sizeof(long),
            StorageColumnType.Guid => 16,
            StorageColumnType.Vector => sizeof(int) + (cell.VectorLength * sizeof(float)),
            _ => MeasureString(TextOf(cell)),
        };
    }

    /// <summary>
    /// Writes one cell (its null flag, then its value) exactly as <see cref="SerializeCells(IReadOnlyList{Models.Catalog.Column}, ReadOnlySpan{CellValue})"/>
    /// would, and returns the bytes written. <paramref name="destination"/> must hold <see cref="MeasureCell"/> bytes.
    /// </summary>
    internal static int WriteCell(StorageColumnType kind, CellValue cell, Span<byte> destination)
    {
        if (cell.IsNull)
        {
            destination[0] = 1;
            return 1;
        }

        destination[0] = 0;
        Span<byte> value = destination[1..];
        switch (kind)
        {
            case StorageColumnType.Int:
                BinaryPrimitives.WriteInt32LittleEndian(value, checked((int)IntegerOf(cell)));
                return 1 + sizeof(int);
            case StorageColumnType.Float:
                BinaryPrimitives.WriteInt32LittleEndian(value, SingleBits((float)DoubleOf(cell)));
                return 1 + sizeof(int);
            case StorageColumnType.BigInt:
                BinaryPrimitives.WriteInt64LittleEndian(value, IntegerOf(cell));
                return 1 + sizeof(long);
            case StorageColumnType.Double:
                BinaryPrimitives.WriteInt64LittleEndian(value, BitConverter.DoubleToInt64Bits(DoubleOf(cell)));
                return 1 + sizeof(long);
            case StorageColumnType.Bit:
                value[0] = cell.AsBoolean() ? (byte)1 : (byte)0;
                return 1 + sizeof(bool);
            case StorageColumnType.Date:
            case StorageColumnType.DateTime:
                BinaryPrimitives.WriteInt64LittleEndian(value, cell.AsDate().ToDateTime().ToBinary());
                return 1 + sizeof(long);
            case StorageColumnType.Guid:
                if (!cell.AsGuid().TryWriteBytes(value))
                {
                    throw new ArgumentException("Destination is too short for a GUID cell.", nameof(destination));
                }

                return 1 + 16;
            case StorageColumnType.Vector:
            {
                ReadOnlySpan<float> vector = cell.AsVectorReadOnlySpan();
                BinaryPrimitives.WriteInt32LittleEndian(value, vector.Length);
                for (int i = 0; i < vector.Length; i++)
                {
                    BinaryPrimitives.WriteInt32LittleEndian(value.Slice(sizeof(int) + (i * sizeof(float))), SingleBits(vector[i]));
                }

                return 1 + sizeof(int) + (vector.Length * sizeof(float));
            }
            default:
                return 1 + WriteString(TextOf(cell), value);
        }
    }

    private static int SingleBits(float value) => BitConverter.SingleToInt32Bits(value);

    private static long IntegerOf(CellValue cell) => cell.Type switch
    {
        CellType.Int32 => cell.AsInt32(),
        CellType.Int64 => cell.AsInt64(),
        CellType.Boolean => cell.AsBoolean() ? 1 : 0,
        CellType.Double => checked((long)cell.AsDouble()),
        CellType.Decimal => decimal.ToInt64(cell.AsDecimal()),
        _ => throw new InvalidDataException($"A {cell.Type} cell cannot be stored in an integer column."),
    };

    private static double DoubleOf(CellValue cell) => cell.Type switch
    {
        CellType.Double => cell.AsDouble(),
        CellType.Int32 => cell.AsInt32(),
        CellType.Int64 => cell.AsInt64(),
        CellType.Decimal => (double)cell.AsDecimal(),
        _ => throw new InvalidDataException($"A {cell.Type} cell cannot be stored in a floating-point column."),
    };

    private static string TextOf(CellValue cell) => cell.Type switch
    {
        CellType.String => cell.AsString() ?? string.Empty,
        CellType.Int32 => cell.AsInt32().ToString(CultureInfo.InvariantCulture),
        CellType.Int64 => cell.AsInt64().ToString(CultureInfo.InvariantCulture),
        CellType.Double => cell.AsDouble().ToString("R", CultureInfo.InvariantCulture),
        CellType.Decimal => cell.AsDecimal().ToString(CultureInfo.InvariantCulture),
        CellType.Boolean => cell.AsBoolean() ? "True" : "False",
        CellType.Guid => cell.AsGuid().ToString(),
        CellType.Date => cell.AsDate().ToString(),
        _ => throw new InvalidDataException($"A {cell.Type} cell cannot be stored in a text column."),
    };

    // BinaryWriter.Write(string): a 7-bit-encoded byte count, then the UTF-8 bytes.
    private static int MeasureString(string text)
    {
        int length = Encoding.UTF8.GetByteCount(text);
        return Measure7BitInt(length) + length;
    }

    private static int WriteString(string text, Span<byte> destination)
    {
        int length = Encoding.UTF8.GetByteCount(text);
        int prefix = 0;
        uint remaining = (uint)length;
        while (remaining >= 0x80)
        {
            destination[prefix++] = (byte)(remaining | 0x80);
            remaining >>= 7;
        }

        destination[prefix++] = (byte)remaining;
        return prefix + Encoding.UTF8.GetBytes(text.AsSpan(), destination[prefix..]);
    }

    private static int Measure7BitInt(int value)
    {
        int bytes = 1;
        for (uint remaining = (uint)value; remaining >= 0x80; remaining >>= 7)
        {
            bytes++;
        }

        return bytes;
    }
}
