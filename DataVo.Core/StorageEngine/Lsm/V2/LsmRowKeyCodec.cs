using System.Buffers.Binary;

namespace DataVo.Core.StorageEngine.Lsm.V2;

/// <summary>SSTable format a table writes on flush and compaction.</summary>
internal enum LsmSstableFormat
{
    /// <summary>Row-keyed (v6/v8: 16-byte row keys), page-aligned blocks, values.</summary>
    RowKeyed = 2,

    /// <summary>Keyed (v5: variable byte keys), page-aligned blocks; used by index tables.</summary>
    Keyed = 3,
}

/// <summary>
/// Encodings shared by the engine, the flush path, and compaction: row keys inside internal keys
/// (the 16 big-endian bytes of a <see cref="RowKey"/>, order-preserving) and the per-table metadata entry
/// (next row id and live-row count) that every commit writes alongside its rows.
/// </summary>
internal static class LsmRowKeyCodec
{
    public const int RowKeySize = 16;

    /// <summary>User key of the table metadata entry ("__datavo_next_row_id").</summary>
    public static readonly byte[] TableMetadataKey =
        [0x5F, 0x5F, 0x64, 0x61, 0x74, 0x61, 0x76, 0x6F, 0x5F, 0x6E, 0x65, 0x78, 0x74, 0x5F, 0x72, 0x6F, 0x77, 0x5F, 0x69, 0x64];

    public static void EncodeRowKey(RowKey key, Span<byte> destination) => key.WriteTo(destination);

    /// <summary>Decodes a 16-byte row user key.</summary>
    [System.Runtime.CompilerServices.MethodImpl(DataVo.Core.Compat.MethodImplCompat.AggressiveOptimization)]
    public static RowKey DecodeRowKey(ReadOnlySpan<byte> userKey) => userKey.Length == RowKeySize
        ? RowKey.Read(userKey)
        : throw new InvalidDataException($"LSM storage engine expected a 16-byte row key, got {userKey.Length} bytes.");

    public static bool IsRowKey(ReadOnlySpan<byte> userKey) => userKey.Length == RowKeySize;

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

    /// <summary>Decodes a metadata value written by <see cref="EncodeTableMetadata"/>.</summary>
    public static (long NextRowId, long LiveRowCount) DecodeTableMetadata(ReadOnlySpan<byte> value)
    {
        if (value.Length != sizeof(long) * 2)
        {
            throw new InvalidDataException("LSM table metadata value is malformed.");
        }

        long nextRowId = BinaryPrimitives.ReadInt64LittleEndian(value);
        if (nextRowId < 1)
        {
            throw new InvalidDataException("LSM next-row-id metadata value must be positive.");
        }

        return (nextRowId, BinaryPrimitives.ReadInt64LittleEndian(value[sizeof(long)..]));
    }

    /// <summary>
    /// Streams one SSTable's worth of entries (rows and metadata versions, in internal-key order) into a
    /// row writer: rows become entries, the newest metadata version becomes the properties.
    /// </summary>
    [System.Runtime.CompilerServices.MethodImpl(DataVo.Core.Compat.MethodImplCompat.AggressiveOptimization)]
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
