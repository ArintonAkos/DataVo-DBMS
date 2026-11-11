using System.Buffers.Binary;
using System.ComponentModel;
using System.Runtime.CompilerServices;

namespace DataVo.Core;

/// <summary>
/// Reads one value from a stored row at a byte offset. Used by the conditions <c>DataVo.Generators</c> compiles,
/// which are given offsets the scan has already checked against the row; application code does not call these.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public static class RowBytes
{
    /// <summary>Reads an <c>INT</c> value.</summary>
    /// <param name="row">The stored row.</param>
    /// <param name="offset">The offset of the value's first byte.</param>
    /// <returns>The value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int Int32(ReadOnlySpan<byte> row, int offset) => BinaryPrimitives.ReadInt32LittleEndian(row.Slice(offset));

    /// <summary>Reads a <c>BIGINT</c> value.</summary>
    /// <param name="row">The stored row.</param>
    /// <param name="offset">The offset of the value's first byte.</param>
    /// <returns>The value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long Int64(ReadOnlySpan<byte> row, int offset) => BinaryPrimitives.ReadInt64LittleEndian(row.Slice(offset));

    /// <summary>Reads a <c>FLOAT</c> value.</summary>
    /// <param name="row">The stored row.</param>
    /// <param name="offset">The offset of the value's first byte.</param>
    /// <returns>The value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static float Single(ReadOnlySpan<byte> row, int offset) =>
        BitConverter.Int32BitsToSingle(BinaryPrimitives.ReadInt32LittleEndian(row.Slice(offset)));

    /// <summary>Reads a <c>DOUBLE</c> value.</summary>
    /// <param name="row">The stored row.</param>
    /// <param name="offset">The offset of the value's first byte.</param>
    /// <returns>The value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double Double(ReadOnlySpan<byte> row, int offset) =>
        BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(row.Slice(offset)));

    /// <summary>Reads a <c>BIT</c> value.</summary>
    /// <param name="row">The stored row.</param>
    /// <param name="offset">The offset of the value's byte.</param>
    /// <returns>The value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool Boolean(ReadOnlySpan<byte> row, int offset) => row[offset] != 0;

    /// <summary>Reads a <c>GUID</c> value.</summary>
    /// <param name="row">The stored row.</param>
    /// <param name="offset">The offset of the value's first byte.</param>
    /// <returns>The value.</returns>
    public static Guid Guid(ReadOnlySpan<byte> row, int offset) => new(row.Slice(offset, 16));
}
