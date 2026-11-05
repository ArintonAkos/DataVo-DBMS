using System.Buffers.Binary;

namespace DataVo.Core.StorageEngine.Lsm.V2;

/// <summary>
/// Encodings shared by the engine, the flush path, and compaction: row keys inside internal keys
/// (8-byte, order-preserving) and the per-table metadata entry (next row id and live-row count) that
/// every commit writes alongside its rows.
/// </summary>
internal static class LsmRowKeyCodec
{
    public const int RowKeySize = sizeof(long);

    /// <summary>User key of the table metadata entry ("__datavo_next_row_id").</summary>
    public static readonly byte[] TableMetadataKey =
        [0x5F, 0x5F, 0x64, 0x61, 0x74, 0x61, 0x76, 0x6F, 0x5F, 0x6E, 0x65, 0x78, 0x74, 0x5F, 0x72, 0x6F, 0x77, 0x5F, 0x69, 0x64];

    public static void EncodeRowId(long rowId, Span<byte> destination) =>
        InternalKey.EncodeInt64UserKey(destination, rowId);

    public static long DecodeRowId(ReadOnlySpan<byte> userKey)
    {
        if (userKey.Length != RowKeySize)
        {
            throw new InvalidDataException("LSM storage engine expected an 8-byte RowId user key.");
        }

        ulong flipped = BinaryPrimitives.ReadUInt64BigEndian(userKey);
        return unchecked((long)(flipped ^ (ulong)long.MinValue));
    }

    public static bool IsRowKey(ReadOnlySpan<byte> userKey) => userKey.Length == RowKeySize;

    public static bool IsTableMetadataKey(ReadOnlySpan<byte> userKey) => userKey.SequenceEqual(TableMetadataKey);

    /// <summary>Internal key (user key ‖ tag) of a row version.</summary>
    public static byte[] RowInternalKey(long rowId, ulong sequence, LsmValueType type)
    {
        var key = new byte[RowKeySize + InternalKey.TagSize];
        Span<byte> userKey = stackalloc byte[RowKeySize];
        EncodeRowId(rowId, userKey);
        InternalKey.Write(key, userKey, sequence, type);
        return key;
    }

    /// <summary>Internal key of the metadata entry at <paramref name="sequence"/>.</summary>
    public static byte[] MetadataInternalKey(ulong sequence)
    {
        var key = new byte[TableMetadataKey.Length + InternalKey.TagSize];
        InternalKey.Write(key, TableMetadataKey, sequence, LsmValueType.Put);
        return key;
    }

    /// <summary>Metadata value: next row id and live-row count (16 bytes).</summary>
    public static byte[] EncodeTableMetadata(long nextRowId, long liveRowCount)
    {
        var value = new byte[sizeof(long) * 2];
        BinaryPrimitives.WriteInt64LittleEndian(value, nextRowId);
        BinaryPrimitives.WriteInt64LittleEndian(value.AsSpan(sizeof(long)), liveRowCount);
        return value;
    }

    /// <summary>
    /// Decodes a metadata value. The 8-byte form written before live counts were tracked yields
    /// <see cref="SsTableV2Properties.UnknownLiveRowCount"/>.
    /// </summary>
    public static (long NextRowId, long LiveRowCount) DecodeTableMetadata(ReadOnlySpan<byte> value)
    {
        if (value.Length != sizeof(long) && value.Length != sizeof(long) * 2)
        {
            throw new InvalidDataException("LSM table metadata value is malformed.");
        }

        long nextRowId = BinaryPrimitives.ReadInt64LittleEndian(value);
        if (nextRowId < 1)
        {
            throw new InvalidDataException("LSM next-row-id metadata value must be positive.");
        }

        long liveRowCount = value.Length == sizeof(long) * 2
            ? BinaryPrimitives.ReadInt64LittleEndian(value[sizeof(long)..])
            : SsTableV2Properties.UnknownLiveRowCount;
        return (nextRowId, liveRowCount);
    }

    /// <summary>
    /// Streams one SSTable's worth of entries (rows and metadata versions, in internal-key order) into a
    /// v2 writer: rows become entries, the newest metadata version becomes the properties.
    /// </summary>
    public static void AddInternalEntry(SsTableV2Writer writer, ReadOnlySpan<byte> internalKey, ReadOnlySpan<byte> value)
    {
        ReadOnlySpan<byte> userKey = InternalKey.UserKey(internalKey);
        ulong sequence = InternalKey.Sequence(internalKey);
        LsmValueType type = InternalKey.ValueType(internalKey);
        if (IsRowKey(userKey))
        {
            writer.Add(DecodeRowId(userKey), sequence, type, type == LsmValueType.Put ? value : ReadOnlySpan<byte>.Empty);
            return;
        }

        if (IsTableMetadataKey(userKey) && type == LsmValueType.Put)
        {
            (long nextRowId, long liveRowCount) = DecodeTableMetadata(value);
            writer.SetTableMetadata(nextRowId, liveRowCount, sequence);
            return;
        }

        throw new InvalidDataException($"Row-keyed SSTable got an unexpected {userKey.Length}-byte user key.");
    }
}
