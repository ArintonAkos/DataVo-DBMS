using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.Core.StorageEngine.Memory;

/// <summary>
/// Reads and writes the values of a packed in-memory row: fixed-width values in the row's record, strings and
/// vectors among the row's object references. Every reader of packed rows goes through these helpers, so there
/// is one definition of how each column type is stored.
/// </summary>
/// <remarks>
/// A row's references are a run inside a shared array: <c>references[referenceBase + slot]</c> is the row's
/// reference number <c>slot</c>. A layout with no reference columns never touches the array, so an empty one
/// will do. Values are stored in the machine's own byte order, since records never leave memory.
/// <para>
/// There are three levels. The value helpers (<see cref="ReadInt32"/>, <see cref="WriteString"/> and so on)
/// take an offset or a slot from a <see cref="RowField"/> and are what code that knows a column's type at
/// compile time calls. <see cref="ReadCell"/> and <see cref="TryWriteCell"/> handle one column of any type,
/// NULL included. <see cref="Decode"/> and <see cref="TryEncode"/> handle a whole row.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// RowLayout layout = RowLayout.Create([CellType.Int32, CellType.String]);
/// Span&lt;byte&gt; record = stackalloc byte[layout.RecordBytes];
/// var references = new object?[layout.ReferenceCount];
/// FlatRowCodec.TryEncode(layout, [CellValue.From(7), CellValue.From("ada")], record, references, 0);
///
/// int id = FlatRowCodec.ReadInt32(record, layout[0].Position);                  // 7
/// string? name = FlatRowCodec.ReadString(references, 0, layout[1].Position);    // "ada"
/// </code>
/// </example>
internal static class FlatRowCodec
{
    /// <summary>Reads a BIT value.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool ReadBoolean(ReadOnlySpan<byte> record, int offset) => record[offset] != 0;

    /// <summary>Reads an INT value.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ReadInt32(ReadOnlySpan<byte> record, int offset) => Read<int>(record, offset);

    /// <summary>Reads a BIGINT value.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long ReadInt64(ReadOnlySpan<byte> record, int offset) => Read<long>(record, offset);

    /// <summary>Reads a floating-point value.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double ReadDouble(ReadOnlySpan<byte> record, int offset) => Read<double>(record, offset);

    /// <summary>Reads a DATE value.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DataVoDate ReadDate(ReadOnlySpan<byte> record, int offset) =>
        DataVoDate.FromDayNumber(Read<int>(record, offset));

    /// <summary>Reads a DECIMAL value.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static decimal ReadDecimal(ReadOnlySpan<byte> record, int offset) => Read<decimal>(record, offset);

    /// <summary>Reads a GUID value.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Guid ReadGuid(ReadOnlySpan<byte> record, int offset) => Read<Guid>(record, offset);

    /// <summary>Reads a string; <see langword="null"/> for NULL.</summary>
    /// <param name="references">The array holding the row's references.</param>
    /// <param name="referenceBase">The index of the row's first reference.</param>
    /// <param name="slot">The column's reference slot.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string? ReadString(object?[] references, int referenceBase, int slot) =>
        (string?)Volatile.Read(ref references[referenceBase + slot]);

    /// <summary>
    /// Reads a vector; <see langword="null"/> for NULL. The array is the stored one, not a copy: the caller
    /// must not write to it.
    /// </summary>
    /// <param name="references">The array holding the row's references.</param>
    /// <param name="referenceBase">The index of the row's first reference.</param>
    /// <param name="slot">The column's reference slot.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static float[]? ReadVector(object?[] references, int referenceBase, int slot) =>
        (float[]?)Volatile.Read(ref references[referenceBase + slot]);

    /// <summary>Writes a BIT value.</summary>
    public static void WriteBoolean(Span<byte> record, int offset, bool value) => record[offset] = value ? (byte)1 : (byte)0;

    /// <summary>Writes an INT value.</summary>
    public static void WriteInt32(Span<byte> record, int offset, int value) => Write(record, offset, value);

    /// <summary>Writes a BIGINT value.</summary>
    public static void WriteInt64(Span<byte> record, int offset, long value) => Write(record, offset, value);

    /// <summary>Writes a floating-point value.</summary>
    public static void WriteDouble(Span<byte> record, int offset, double value) => Write(record, offset, value);

    /// <summary>Writes a DATE value.</summary>
    public static void WriteDate(Span<byte> record, int offset, DataVoDate value) => Write(record, offset, value.DayNumber);

    /// <summary>Writes a DECIMAL value.</summary>
    public static void WriteDecimal(Span<byte> record, int offset, decimal value) => Write(record, offset, value);

    /// <summary>Writes a GUID value.</summary>
    public static void WriteGuid(Span<byte> record, int offset, Guid value) => Write(record, offset, value);

    /// <summary>Stores a string.</summary>
    public static void WriteString(object?[] references, int referenceBase, int slot, string? value) =>
        references[referenceBase + slot] = value;

    /// <summary>Stores a vector. The array itself is kept, so it must not be written to afterwards.</summary>
    public static void WriteVector(object?[] references, int referenceBase, int slot, float[]? value) =>
        references[referenceBase + slot] = value;

    /// <summary>Reads one column of a row as a cell; the NULL cell when the column is NULL.</summary>
    /// <param name="layout">The row's layout.</param>
    /// <param name="ordinal">The column.</param>
    /// <param name="record">The row's record.</param>
    /// <param name="references">The array holding the row's references.</param>
    /// <param name="referenceBase">The index of the row's first reference.</param>
    public static CellValue ReadCell(RowLayout layout, int ordinal, ReadOnlySpan<byte> record, object?[] references, int referenceBase)
    {
        if (layout.IsNull(record, ordinal))
        {
            return CellValue.Null;
        }

        RowField field = layout[ordinal];
        return field.Type switch
        {
            CellType.Boolean => CellValue.From(ReadBoolean(record, field.Position)),
            CellType.Int32 => CellValue.From(ReadInt32(record, field.Position)),
            CellType.Int64 => CellValue.From(ReadInt64(record, field.Position)),
            CellType.Double => CellValue.From(ReadDouble(record, field.Position)),
            CellType.Date => CellValue.From(ReadDate(record, field.Position)),
            CellType.Decimal => CellValue.From(ReadDecimal(record, field.Position)),
            CellType.Guid => CellValue.From(ReadGuid(record, field.Position)),
            CellType.String => CellValue.From(ReadString(references, referenceBase, field.Position)),
            _ => CellValue.FromVectorOwned(ReadVector(references, referenceBase, field.Position)),
        };
    }

    /// <summary>
    /// Writes one column of a row from a cell. A NULL cell sets the column's null bit and writes no value.
    /// </summary>
    /// <returns>
    /// <see langword="false"/>, having written nothing, when the cell is neither NULL nor of the column's type.
    /// </returns>
    public static bool TryWriteCell(RowLayout layout, int ordinal, in CellValue cell, Span<byte> record, object?[] references, int referenceBase)
    {
        if (cell.IsNull)
        {
            layout.SetNull(record, ordinal);
            return true;
        }

        RowField field = layout[ordinal];
        if (cell.Type != field.Type)
        {
            return false;
        }

        WriteValue(field, cell, record, references, referenceBase);
        return true;
    }

    /// <summary>Reads a whole row into cells, one per column.</summary>
    /// <param name="layout">The row's layout.</param>
    /// <param name="record">The row's record.</param>
    /// <param name="references">The array holding the row's references.</param>
    /// <param name="referenceBase">The index of the row's first reference.</param>
    /// <param name="cells">Receives the cells; at least as long as the column count.</param>
    public static void Decode(RowLayout layout, ReadOnlySpan<byte> record, object?[] references, int referenceBase, Span<CellValue> cells)
    {
        for (int ordinal = 0; ordinal < layout.ColumnCount; ordinal++)
        {
            cells[ordinal] = ReadCell(layout, ordinal, record, references, referenceBase);
        }
    }

    /// <summary>
    /// Writes a whole row from cells into a zeroed record and cleared references. The record's status byte is
    /// left alone: the caller marks the row live once it is ready to be seen.
    /// </summary>
    /// <returns>
    /// <see langword="false"/> when some cell is not of its column's type, so the row cannot be packed. The
    /// record and the references are then zeroed and cleared again.
    /// </returns>
    /// <exception cref="ArgumentException">The number of cells is not the number of columns.</exception>
    public static bool TryEncode(RowLayout layout, ReadOnlySpan<CellValue> cells, Span<byte> record, object?[] references, int referenceBase)
    {
        if (cells.Length != layout.ColumnCount)
        {
            throw new ArgumentException($"Row has {cells.Length} cells but the table has {layout.ColumnCount} columns.", nameof(cells));
        }

        for (int ordinal = 0; ordinal < cells.Length; ordinal++)
        {
            if (!TryWriteCell(layout, ordinal, in cells[ordinal], record, references, referenceBase))
            {
                Erase(layout, record, references, referenceBase);
                return false;
            }
        }

        return true;
    }

    /// <summary>Zeroes a row's record and clears its references.</summary>
    public static void Erase(RowLayout layout, Span<byte> record, object?[] references, int referenceBase)
    {
        record.Slice(0, layout.RecordBytes).Clear();
        ClearReferences(layout, references, referenceBase);
    }

    /// <summary>
    /// Clears a row's references so the strings and vectors they held can be collected. Readers that were
    /// already looking at the row see the change.
    /// </summary>
    public static void ClearReferences(RowLayout layout, object?[] references, int referenceBase)
    {
        for (int slot = 0; slot < layout.ReferenceCount; slot++)
        {
            Volatile.Write(ref references[referenceBase + slot], null);
        }
    }

    private static void WriteValue(RowField field, in CellValue cell, Span<byte> record, object?[] references, int referenceBase)
    {
        switch (field.Type)
        {
            case CellType.Boolean: WriteBoolean(record, field.Position, cell.AsBoolean()); break;
            case CellType.Int32: WriteInt32(record, field.Position, cell.AsInt32()); break;
            case CellType.Int64: WriteInt64(record, field.Position, cell.AsInt64()); break;
            case CellType.Double: WriteDouble(record, field.Position, cell.AsDouble()); break;
            case CellType.Date: WriteDate(record, field.Position, cell.AsDate()); break;
            case CellType.Decimal: WriteDecimal(record, field.Position, cell.AsDecimal()); break;
            case CellType.Guid: WriteGuid(record, field.Position, cell.AsGuid()); break;
            case CellType.String: WriteString(references, referenceBase, field.Position, cell.AsString()); break;
            default: WriteVector(references, referenceBase, field.Position, cell.AsVectorShared()); break;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static T Read<T>(ReadOnlySpan<byte> record, int offset)
        where T : unmanaged =>
        Unsafe.ReadUnaligned<T>(ref MemoryMarshal.GetReference(record.Slice(offset, Unsafe.SizeOf<T>())));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Write<T>(Span<byte> record, int offset, T value)
        where T : unmanaged =>
        Unsafe.WriteUnaligned(ref MemoryMarshal.GetReference(record.Slice(offset, Unsafe.SizeOf<T>())), value);
}
