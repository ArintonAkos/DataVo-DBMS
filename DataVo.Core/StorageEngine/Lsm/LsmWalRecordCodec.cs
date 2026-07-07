using System.Buffers.Binary;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>Owned decoded representation of one LSM WAL mutation payload.</summary>
public readonly record struct LsmWalRecord(
    byte[] UserKey,
    ulong Seqno,
    LsmValueType ValueType,
    byte[] Value);

/// <summary>
/// Binary payload codec for one LSM mutation. The payload carries the user key, sequence number,
/// value type, and value bytes; replay rebuilds internal keys through <see cref="MemTable"/>.
/// </summary>
public static class LsmWalRecordCodec
{
    private const ushort CurrentVersion = 1;
    private const int VersionOffset = 0;
    private const int ValueTypeOffset = 2;
    private const int ReservedOffset = 3;
    private const int SeqnoOffset = 4;
    private const int UserKeyLengthOffset = 12;
    private const int ValueLengthOffset = 16;
    private const int HeaderSize = 20;

    /// <summary>Measures the encoded payload size for the supplied key and value bytes.</summary>
    public static int MeasureSize(ReadOnlySpan<byte> userKey, ReadOnlySpan<byte> value) =>
        MeasureSize(userKey.Length, value.Length);

    /// <summary>Measures the encoded payload size for the supplied key and value lengths.</summary>
    public static int MeasureSize(int userKeyLength, int valueLength)
    {
        if (userKeyLength < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(userKeyLength));
        }

        if (valueLength < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(valueLength));
        }

        return checked(HeaderSize + userKeyLength + valueLength);
    }

    /// <summary>Writes one mutation payload to <paramref name="destination"/>.</summary>
    public static int Write(
        Span<byte> destination,
        ReadOnlySpan<byte> userKey,
        ulong seqno,
        LsmValueType valueType,
        ReadOnlySpan<byte> value)
    {
        if (!IsValidValueType(valueType))
        {
            throw new ArgumentOutOfRangeException(nameof(valueType));
        }

        if (seqno > InternalKey.MaxSequenceNumber)
        {
            throw new ArgumentOutOfRangeException(nameof(seqno));
        }

        if (valueType == LsmValueType.Deletion && !value.IsEmpty)
        {
            throw new ArgumentException("Deletion records must not carry a value.", nameof(value));
        }

        int size = MeasureSize(userKey, value);
        if (destination.Length < size)
        {
            throw new ArgumentException("Destination is too small for the LSM WAL payload.", nameof(destination));
        }

        BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(VersionOffset, sizeof(ushort)), CurrentVersion);
        destination[ValueTypeOffset] = (byte)valueType;
        destination[ReservedOffset] = 0;
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(SeqnoOffset, sizeof(ulong)), seqno);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(UserKeyLengthOffset, sizeof(int)), userKey.Length);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(ValueLengthOffset, sizeof(int)), value.Length);
        userKey.CopyTo(destination.Slice(HeaderSize, userKey.Length));
        value.CopyTo(destination.Slice(HeaderSize + userKey.Length, value.Length));

        return size;
    }

    /// <summary>Attempts to decode one mutation payload into owned key and value arrays.</summary>
    public static bool TryRead(ReadOnlySpan<byte> source, out LsmWalRecord record)
    {
        record = default;
        if (!TryReadHeader(source, out LsmValueType valueType, out ulong seqno, out int userKeyLength, out int valueLength))
        {
            return false;
        }

        byte[] userKey = source.Slice(HeaderSize, userKeyLength).ToArray();
        byte[] value = source.Slice(HeaderSize + userKeyLength, valueLength).ToArray();
        record = new LsmWalRecord(userKey, seqno, valueType, value);
        return true;
    }

    /// <summary>Applies an already-decoded mutation to <paramref name="memTable"/>.</summary>
    public static bool Apply(LsmWalRecord record, MemTable memTable)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(memTable);

        if (record.UserKey is null || record.Value is null)
        {
            return false;
        }

        if (!IsValidValueType(record.ValueType))
        {
            return false;
        }

        if (record.Seqno > InternalKey.MaxSequenceNumber)
        {
            return false;
        }

        if (record.ValueType == LsmValueType.Deletion)
        {
            if (record.Value.Length != 0)
            {
                return false;
            }

            memTable.Delete(record.UserKey, record.Seqno);
            return true;
        }

        memTable.Put(record.UserKey, record.Seqno, LsmValueType.Put, record.Value);
        return true;
    }

    /// <summary>Decodes and applies one mutation payload to <paramref name="memTable"/>.</summary>
    public static bool Replay(ReadOnlySpan<byte> source, MemTable memTable)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(memTable);
        return TryRead(source, out LsmWalRecord record) && Apply(record, memTable);
    }

    private static bool TryReadHeader(
        ReadOnlySpan<byte> source,
        out LsmValueType valueType,
        out ulong seqno,
        out int userKeyLength,
        out int valueLength)
    {
        valueType = default;
        seqno = default;
        userKeyLength = default;
        valueLength = default;

        if (source.Length < HeaderSize)
        {
            return false;
        }

        ushort version = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(VersionOffset, sizeof(ushort)));
        if (version != CurrentVersion || source[ReservedOffset] != 0)
        {
            return false;
        }

        valueType = (LsmValueType)source[ValueTypeOffset];
        if (!IsValidValueType(valueType))
        {
            return false;
        }

        seqno = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(SeqnoOffset, sizeof(ulong)));
        if (seqno > InternalKey.MaxSequenceNumber)
        {
            return false;
        }

        userKeyLength = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(UserKeyLengthOffset, sizeof(int)));
        valueLength = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(ValueLengthOffset, sizeof(int)));
        if (userKeyLength < 0 || valueLength < 0)
        {
            return false;
        }

        if (valueType == LsmValueType.Deletion && valueLength != 0)
        {
            return false;
        }

        int expectedLength;
        try
        {
            expectedLength = MeasureSize(userKeyLength, valueLength);
        }
        catch (OverflowException)
        {
            return false;
        }

        return expectedLength == source.Length;
    }

    private static bool IsValidValueType(LsmValueType valueType) =>
        valueType is LsmValueType.Deletion or LsmValueType.Put;
}
