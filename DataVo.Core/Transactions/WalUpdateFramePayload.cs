using System.Buffers.Binary;
using System.Text;

namespace DataVo.Core.Transactions;

/// <summary>
/// Binary codec for the payload of a <see cref="WalFrameOperationType.Update"/> WAL frame.
/// </summary>
/// <remarks>
/// The hot-path writer encodes the database name, table name, the pre-update row id, and the full
/// serialized new-row bytes directly into a caller-supplied <see cref="Span{T}"/> with zero heap
/// allocation. Recovery (a cold path) reads the payload back, allocating freely. Carrying the full
/// new row keeps replay self-contained — recovery never has to re-read the pre-update row.
/// </remarks>
/// <para>
/// Layout: <c>[version:1][dbLen:u16][db:utf8][tableLen:u16][table:utf8][oldRowId:i64][rowLen:i32][rowBytes]</c>.
/// </para>
internal static class WalUpdateFramePayload
{
    private const byte Version = 1;
    private const int VersionSize = sizeof(byte);
    private const int LengthPrefixSize = sizeof(ushort);

    /// <summary>
    /// Computes the exact payload size for the supplied identifiers and new-row length.
    /// </summary>
    public static int MeasureSize(string databaseName, string tableName, int newRowLength)
    {
        if (newRowLength < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(newRowLength));
        }

        return VersionSize
            + LengthPrefixSize + Encoding.UTF8.GetByteCount(databaseName)
            + LengthPrefixSize + Encoding.UTF8.GetByteCount(tableName)
            + sizeof(long)
            + sizeof(int) + newRowLength;
    }

    /// <summary>
    /// Encodes the payload into <paramref name="destination"/> with no heap allocation.
    /// </summary>
    /// <returns>The number of bytes written.</returns>
    public static int Write(Span<byte> destination, string databaseName, string tableName, long oldRowId, ReadOnlySpan<byte> newRowBytes)
    {
        int offset = 0;
        destination[offset] = Version;
        offset += VersionSize;

        offset += WriteLengthPrefixedUtf8(destination[offset..], databaseName);
        offset += WriteLengthPrefixedUtf8(destination[offset..], tableName);

        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(offset, sizeof(long)), oldRowId);
        offset += sizeof(long);

        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(offset, sizeof(int)), newRowBytes.Length);
        offset += sizeof(int);

        newRowBytes.CopyTo(destination.Slice(offset, newRowBytes.Length));
        offset += newRowBytes.Length;

        return offset;
    }

    /// <summary>
    /// Decodes a payload written by <see cref="Write"/>. Returns <see langword="false"/> for any
    /// malformed or truncated buffer so recovery can treat it as a torn tail rather than throw.
    /// </summary>
    public static bool TryRead(ReadOnlySpan<byte> payload, out string databaseName, out string tableName, out long oldRowId, out byte[] newRowBytes)
    {
        databaseName = string.Empty;
        tableName = string.Empty;
        oldRowId = 0;
        newRowBytes = [];

        int offset = 0;
        if (payload.Length < VersionSize || payload[offset] != Version)
        {
            return false;
        }

        offset += VersionSize;

        if (!TryReadLengthPrefixedUtf8(payload, ref offset, out databaseName)
            || !TryReadLengthPrefixedUtf8(payload, ref offset, out tableName))
        {
            return false;
        }

        if (offset + sizeof(long) > payload.Length)
        {
            return false;
        }

        oldRowId = BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(offset, sizeof(long)));
        offset += sizeof(long);

        if (offset + sizeof(int) > payload.Length)
        {
            return false;
        }

        int rowLength = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, sizeof(int)));
        offset += sizeof(int);

        if (rowLength < 0 || offset + rowLength > payload.Length)
        {
            return false;
        }

        newRowBytes = payload.Slice(offset, rowLength).ToArray();
        return true;
    }

    private static int WriteLengthPrefixedUtf8(Span<byte> destination, string value)
    {
        int byteCount = Encoding.UTF8.GetByteCount(value);
        if (byteCount > ushort.MaxValue)
        {
            throw new ArgumentException($"Identifier '{value}' exceeds the WAL frame length limit.");
        }

        BinaryPrimitives.WriteUInt16LittleEndian(destination[..LengthPrefixSize], (ushort)byteCount);
        Encoding.UTF8.GetBytes(value, destination.Slice(LengthPrefixSize, byteCount));
        return LengthPrefixSize + byteCount;
    }

    private static bool TryReadLengthPrefixedUtf8(ReadOnlySpan<byte> payload, ref int offset, out string value)
    {
        value = string.Empty;
        if (offset + LengthPrefixSize > payload.Length)
        {
            return false;
        }

        int length = BinaryPrimitives.ReadUInt16LittleEndian(payload.Slice(offset, LengthPrefixSize));
        offset += LengthPrefixSize;

        if (offset + length > payload.Length)
        {
            return false;
        }

        value = Encoding.UTF8.GetString(payload.Slice(offset, length));
        offset += length;
        return true;
    }
}
