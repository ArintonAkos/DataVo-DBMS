using DataVo.Core.Transactions;
using System.Buffers;
using System.Buffers.Binary;
using System.Threading;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>Writes LSM mutation payloads into binary WAL frames without allocating payload buffers.</summary>
internal sealed class LsmWalWriter
{
    private const int LsmTableId = 0;
    private const long UnknownRowId = 0;

    private readonly WalFileStore _store;
    private readonly LsmWalDurabilityMode _durabilityMode;
    private readonly byte[] _frameBuffer;
    private readonly object _commitGate = new();
    private long _nextLsn;
    private long _appendedLsn;  // highest LSN handed to the store; volatile-read by group commit
    private long _durableLsn;   // highest LSN covered by a completed fsync

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

    internal LsmWalDurabilityTicket AppendMutationBuffered(
        ReadOnlySpan<byte> userKey,
        ulong seqno,
        LsmValueType valueType,
        ReadOnlySpan<byte> value)
    {
        AppendMutation(userKey, seqno, valueType, value, flushToDisk: false);
        return _durabilityMode == LsmWalDurabilityMode.StrictFsync
            ? new LsmWalDurabilityTicket(this, Volatile.Read(ref _appendedLsn))
            : LsmWalDurabilityTicket.None;
    }

    /// <summary>
    /// Blocks until every frame appended before <paramref name="lsn"/> (inclusive) is covered by a
    /// completed fsync. Group commit: waiters serialize on a commit gate, the thread holding it acts
    /// as leader and issues one fsync that covers every frame appended so far, and followers that
    /// arrive during that fsync observe the advanced durable watermark and return without issuing
    /// their own. Under N concurrent strict-mode writers, one physical fsync amortizes across the
    /// whole convoy instead of costing one fsync per operation.
    /// </summary>
    internal void EnsureDurableThrough(long lsn)
    {
        if (Volatile.Read(ref _durableLsn) >= lsn)
        {
            return;
        }

        lock (_commitGate)
        {
            while (Volatile.Read(ref _durableLsn) < lsn)
            {
                // Capture the appended watermark BEFORE the fsync: the fsync durably covers at
                // least everything handed to the store up to this point. The concurrent flush does
                // not hold the store lock, so follower appends accumulate DURING the leader's fsync
                // and the next leader covers them all in one flush.
                long target = Volatile.Read(ref _appendedLsn);
                _store.FlushToDiskConcurrent();
                Volatile.Write(ref _durableLsn, target);
            }
        }
    }

    internal LsmWalDurabilityTicket AppendRowIdPutMutationsBatch(ReadOnlySpan<LsmBatchRowPutEntry> entries)
    {
        if (entries.IsEmpty)
        {
            return LsmWalDurabilityTicket.None;
        }

        int offset = 0;
        Span<byte> userKey = stackalloc byte[sizeof(long)];
        foreach (LsmBatchRowPutEntry entry in entries)
        {
            ValidateMutation(entry.Seqno, LsmValueType.Put, entry.Value);
            InternalKey.EncodeInt64UserKey(userKey, entry.RowId);
            offset = WriteBatchFrameOrFlush(offset, userKey, entry.Seqno, entry.Value);
        }

        AppendBatchTail(offset);
        return _durabilityMode == LsmWalDurabilityMode.StrictFsync
            ? new LsmWalDurabilityTicket(this, Volatile.Read(ref _appendedLsn))
            : LsmWalDurabilityTicket.None;
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
            Volatile.Write(ref _appendedLsn, _nextLsn);
            if (flushToDisk)
            {
                Volatile.Write(ref _durableLsn, _nextLsn);
            }
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
                Volatile.Write(ref _appendedLsn, _nextLsn);
            }

            AppendMutation(userKey, seqno, LsmValueType.Put, value, flushToDisk: false);
            return 0;
        }

        if (offset + frameLength > _frameBuffer.Length)
        {
            _store.AppendFrameBytes(_frameBuffer.AsSpan(0, offset), flushToDisk: false);
            Volatile.Write(ref _appendedLsn, _nextLsn);
            offset = 0;
        }

        return offset + WriteMutationFrame(_frameBuffer.AsSpan(offset, frameLength), userKey, seqno, LsmValueType.Put, value);
    }

    private void AppendBatchTail(int offset)
    {
        if (offset > 0)
        {
            _store.AppendFrameBytes(_frameBuffer.AsSpan(0, offset), flushToDisk: false);
        }

        Volatile.Write(ref _appendedLsn, _nextLsn);
    }
}

/// <summary>
/// A claim on WAL durability produced by a buffered strict-mode append. <see cref="Wait"/> blocks
/// until the append is covered by a group-commit fsync; the default ticket (relaxed mode) is a no-op.
/// Waiting must happen OUTSIDE the table's write lock so concurrent writers can share the fsync.
/// </summary>
internal readonly struct LsmWalDurabilityTicket
{
    public static readonly LsmWalDurabilityTicket None = default;

    private readonly LsmWalWriter? _writer;
    private readonly long _lsn;

    internal LsmWalDurabilityTicket(LsmWalWriter writer, long lsn)
    {
        _writer = writer;
        _lsn = lsn;
    }

    public void Wait() => _writer?.EnsureDurableThrough(_lsn);
}
