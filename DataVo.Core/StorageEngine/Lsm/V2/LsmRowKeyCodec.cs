using System.Buffers.Binary;

namespace DataVo.Core.StorageEngine.Lsm.V2;

/// <summary>SSTable format a table writes on flush and compaction.</summary>
internal enum LsmSstableFormat
{
    /// <summary>v1: one data block per file (generic byte keys; tests and legacy files).</summary>
    Legacy = 0,

    /// <summary>Row-keyed (16-byte row keys since v6; v2 and v4 files hold 8-byte row ids), page-aligned blocks, values.</summary>
    RowKeyed = 2,

    /// <summary>v3: keyed (variable byte keys), page-aligned blocks; used by index tables.</summary>
    Keyed = 3,
}

/// <summary>
/// Encodings shared by the engine, the flush path, and compaction: row keys inside internal keys
/// (the 16 big-endian bytes of a <see cref="RowKey"/>, order-preserving) and the per-table metadata entry
/// (next row id and live-row count) that every commit writes alongside its rows.
/// </summary>
/// <remarks>
/// Before row keys were widened, row user keys were 8-byte order-preserving integers. Those still appear
/// in legacy files (v1 SSTables, per-table WALs) and in manifests; <see cref="DecodeRowKey"/> reads both,
/// and <see cref="NormalizeUserKey"/> widens an old key so it compares correctly against new ones.
/// </remarks>
internal static class LsmRowKeyCodec
{
    public const int RowKeySize = 16;

    /// <summary>Size of the integer row user keys written before row keys were widened.</summary>
    public const int LegacyRowKeySize = sizeof(long);

    /// <summary>User key of the table metadata entry ("__datavo_next_row_id").</summary>
    public static readonly byte[] TableMetadataKey =
        [0x5F, 0x5F, 0x64, 0x61, 0x74, 0x61, 0x76, 0x6F, 0x5F, 0x6E, 0x65, 0x78, 0x74, 0x5F, 0x72, 0x6F, 0x77, 0x5F, 0x69, 0x64];

    public static void EncodeRowKey(RowKey key, Span<byte> destination) => key.WriteTo(destination);

    /// <summary>Decodes a row user key: 16 bytes, or a legacy 8-byte integer key.</summary>
    public static RowKey DecodeRowKey(ReadOnlySpan<byte> userKey) => userKey.Length switch
    {
        RowKeySize => RowKey.Read(userKey),
        // The legacy encoding is the sign-flipped integer, exactly the low half of the widened key.
        LegacyRowKeySize => new RowKey(0, BinaryPrimitives.ReadUInt64BigEndian(userKey)),
        _ => throw new InvalidDataException($"LSM storage engine expected a 16-byte row key, got {userKey.Length} bytes."),
    };

    public static bool IsRowKey(ReadOnlySpan<byte> userKey) => userKey.Length is RowKeySize or LegacyRowKeySize;

    /// <summary>
    /// A row user key in its current 16-byte form (a legacy 8-byte key gains eight leading zero bytes, so
    /// byte order equals key order across old and new keys); any other user key is returned as is.
    /// </summary>
    public static byte[] NormalizeUserKey(ReadOnlySpan<byte> userKey)
    {
        if (userKey.Length != LegacyRowKeySize)
        {
            return userKey.ToArray();
        }

        var widened = new byte[RowKeySize];
        userKey.CopyTo(widened.AsSpan(RowKeySize - LegacyRowKeySize));
        return widened;
    }

    public static bool IsTableMetadataKey(ReadOnlySpan<byte> userKey) => userKey.SequenceEqual(TableMetadataKey);

    /// <summary>Internal key (user key ‖ tag) of a row version.</summary>
    public static byte[] RowInternalKey(RowKey rowKey, ulong sequence, LsmValueType type)
    {
        var key = new byte[RowKeySize + InternalKey.TagSize];
        Span<byte> userKey = stackalloc byte[RowKeySize];
        EncodeRowKey(rowKey, userKey);
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
    /// row writer: rows become entries, the newest metadata version becomes the properties.
    /// </summary>
    public static void AddInternalEntry(SsTableV2Writer writer, ReadOnlySpan<byte> internalKey, ReadOnlySpan<byte> value)
    {
        ReadOnlySpan<byte> userKey = InternalKey.UserKey(internalKey);
        ulong sequence = InternalKey.Sequence(internalKey);
        LsmValueType type = InternalKey.ValueType(internalKey);
        if (IsRowKey(userKey))
        {
            writer.Add(DecodeRowKey(userKey), sequence, type, type == LsmValueType.Put ? value : ReadOnlySpan<byte>.Empty);
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
