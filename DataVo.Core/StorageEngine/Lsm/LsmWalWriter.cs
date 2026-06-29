using DataVo.Core.Transactions;
using System.Buffers;
using System.Buffers.Binary;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>Writes LSM mutation payloads into binary WAL frames without allocating payload buffers.</summary>
internal sealed class LsmWalWriter
{
    private const int LsmTableId = 0;
    private const long UnknownRowId = 0;

    private readonly WalFileStore _store;
    private readonly LsmWalDurabilityMode _durabilityMode;
    private readonly byte[] _frameBuffer;
    private long _nextLsn;

    public LsmWalWriter(string walPath, LsmWalDurabilityMode durabilityMode, int capacityBytes = 1 << 20)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(walPath);
        _store = new WalFileStore(walPath);
        _durabilityMode = durabilityMode;
        _frameBuffer = new byte[Math.Max(capacityBytes, WalAppender.FrameHeaderSize)];
    }

    internal int DurableFlushCount => _store.DurableFlushCount;

    public void AppendMutation(
        ReadOnlySpan<byte> userKey,
        ulong seqno,
        LsmValueType valueType,
        ReadOnlySpan<byte> value)
    {
        AppendMutation(userKey, seqno, valueType, value, flushToDisk: _durabilityMode == LsmWalDurabilityMode.StrictFsync);
    }

    internal void AppendMutationBuffered(
        ReadOnlySpan<byte> userKey,
        ulong seqno,
        LsmValueType valueType,
        ReadOnlySpan<byte> value)
    {
        AppendMutation(userKey, seqno, valueType, value, flushToDisk: false);
    }

    internal void AppendPutMutationsBatch(IReadOnlyList<LsmBatchPutEntry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        if (entries.Count == 0)
        {
            return;
        }

        int offset = 0;
        foreach (LsmBatchPutEntry entry in entries)
        {
            ValidateMutation(entry.Seqno, LsmValueType.Put, entry.Value);
            offset = WriteBatchFrameOrFlush(offset, entry.UserKey, entry.Seqno, entry.Value);
        }

        FlushBatchBuffer(offset);
    }

    internal void AppendRowIdPutMutationsBatch(IReadOnlyList<LsmBatchRowPutEntry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        if (entries.Count == 0)
        {
            return;
        }

        int offset = 0;
        Span<byte> userKey = stackalloc byte[sizeof(long)];
        foreach (LsmBatchRowPutEntry entry in entries)
        {
            ValidateMutation(entry.Seqno, LsmValueType.Put, entry.Value);
            InternalKey.EncodeInt64UserKey(userKey, entry.RowId);
            offset = WriteBatchFrameOrFlush(offset, userKey, entry.Seqno, entry.Value);
        }

        FlushBatchBuffer(offset);
    }

    internal void FlushBufferedMutations()
    {
        if (_durabilityMode == LsmWalDurabilityMode.StrictFsync)
        {
            _store.FlushToDisk();
        }
    }

    private void AppendMutation(
        ReadOnlySpan<byte> userKey,
        ulong seqno,
        LsmValueType valueType,
        ReadOnlySpan<byte> value,
        bool flushToDisk)
    {
        ValidateMutation(seqno, valueType, value);

        int payloadLength = LsmWalRecordCodec.MeasureSize(userKey, value);
        int frameLength = checked(WalAppender.FrameHeaderSize + payloadLength);
        byte[]? rented = null;
        Span<byte> frame = frameLength <= _frameBuffer.Length
            ? _frameBuffer.AsSpan(0, frameLength)
            : (rented = ArrayPool<byte>.Shared.Rent(frameLength)).AsSpan(0, frameLength);

        try
        {
            int written = WriteMutationFrame(frame, userKey, seqno, valueType, value);
            _store.AppendFrameBytes(frame[..written], flushToDisk);
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    public void ReplayInto(MemTable memTable)
    {
        ArgumentNullException.ThrowIfNull(memTable);

        foreach (WalFrameRecord frame in _store.ReadBinaryFrames())
        {
            _ = LsmWalRecordCodec.Replay(frame.Payload, memTable);
        }
    }

    public void Clear()
    {
        _store.DeleteIfExists();
    }

    private static void ValidateMutation(ulong seqno, LsmValueType valueType, ReadOnlySpan<byte> value)
    {
        if (valueType is not (LsmValueType.Put or LsmValueType.Deletion))
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
    }

    private int WriteMutationFrame(
        Span<byte> frame,
        ReadOnlySpan<byte> userKey,
        ulong seqno,
        LsmValueType valueType,
        ReadOnlySpan<byte> value)
    {
        int payloadLength = LsmWalRecordCodec.MeasureSize(userKey, value);
        int frameLength = checked(WalAppender.FrameHeaderSize + payloadLength);
        if (frame.Length < frameLength)
        {
            throw new ArgumentException("Destination frame buffer is too small.", nameof(frame));
        }

        Span<byte> target = frame[..frameLength];
        target[..WalAppender.FrameHeaderSize].Clear();

        BinaryPrimitives.WriteInt32LittleEndian(target[..sizeof(int)], frameLength);
        BinaryPrimitives.WriteInt64LittleEndian(target.Slice(8, sizeof(long)), checked(++_nextLsn));
        target[16] = valueType == LsmValueType.Deletion
            ? (byte)WalFrameOperationType.Delete
            : (byte)WalFrameOperationType.Insert;
        target[17] = 0;
        BinaryPrimitives.WriteUInt16LittleEndian(target.Slice(18, sizeof(ushort)), 0);
        BinaryPrimitives.WriteInt64LittleEndian(target.Slice(20, sizeof(long)), UnknownRowId);
        BinaryPrimitives.WriteInt32LittleEndian(target.Slice(28, sizeof(int)), LsmTableId);

        LsmWalRecordCodec.Write(target.Slice(WalAppender.FrameHeaderSize, payloadLength), userKey, seqno, valueType, value);

        uint checksum = WalCrc32C.HashToUInt32(target[8..frameLength]);
        BinaryPrimitives.WriteUInt32LittleEndian(target.Slice(4, sizeof(uint)), checksum);
        return frameLength;
    }

    private int WriteBatchFrameOrFlush(
        int offset,
        ReadOnlySpan<byte> userKey,
        ulong seqno,
        ReadOnlySpan<byte> value)
    {
        int frameLength = checked(WalAppender.FrameHeaderSize + LsmWalRecordCodec.MeasureSize(userKey, value));
        if (frameLength > _frameBuffer.Length)
        {
            if (offset > 0)
            {
                _store.AppendFrameBytes(_frameBuffer.AsSpan(0, offset), flushToDisk: false);
            }

            AppendMutation(userKey, seqno, LsmValueType.Put, value, flushToDisk: false);
            return 0;
        }

        if (offset + frameLength > _frameBuffer.Length)
        {
            _store.AppendFrameBytes(_frameBuffer.AsSpan(0, offset), flushToDisk: false);
            offset = 0;
        }

        return offset + WriteMutationFrame(_frameBuffer.AsSpan(offset, frameLength), userKey, seqno, LsmValueType.Put, value);
    }

    private void FlushBatchBuffer(int offset)
    {
        if (offset > 0)
        {
            _store.AppendFrameBytes(_frameBuffer.AsSpan(0, offset), flushToDisk: false);
        }

        if (_durabilityMode == LsmWalDurabilityMode.StrictFsync)
        {
            _store.FlushToDisk();
        }
    }
}
