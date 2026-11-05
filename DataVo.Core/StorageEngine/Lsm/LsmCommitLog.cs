using System.Buffers;
using System.Buffers.Binary;
using DataVo.Core.Transactions;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>One recovered commit-log frame: its LSN and the mutations it carries.</summary>
internal readonly record struct LsmCommitFrame(long Lsn, IReadOnlyList<LsmCommitOperation> Operations);

/// <summary>
/// The single write-ahead log of an Lsm data directory (<c>commit.wal</c>). Each appended
/// <see cref="LsmCommitBatch"/> becomes one checksummed frame whose LSN is the sequence number of all
/// its mutations, so a batch is either fully recovered or not at all. Strict mode shares fsyncs
/// between concurrent committers (leader/follower group commit).
/// </summary>
internal sealed class LsmCommitLog : IDisposable
{
    private readonly WalFileStore _store;
    private readonly LsmWalDurabilityMode _durabilityMode;
    private readonly object _appendGate = new();
    private readonly object _commitGate = new();
    private long _lastLsn;      // highest LSN assigned; guarded by _appendGate
    private long _appendedLsn;  // highest LSN handed to the OS; volatile-read by group commit
    private long _durableLsn;   // highest LSN covered by a completed fsync
    private bool _disposed;

    public LsmCommitLog(string path, LsmWalDurabilityMode durabilityMode)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNullOrWhiteSpace(path);
        _store = new WalFileStore(path);
        _durabilityMode = durabilityMode;

        long validBytes = 0;
        foreach (WalFrameRecord frame in _store.ReadBinaryFrames())
        {
            _lastLsn = frame.Header.Lsn;
            validBytes += frame.Header.FrameLength;
        }

        // A crash can leave a partly written frame at the end. Cut it off now: a frame appended after
        // it would sit behind bytes that recovery stops at, and would be lost on the next open.
        if (File.Exists(path) && new FileInfo(path).Length != validBytes)
        {
            _store.TruncateThroughLsn(0);
        }

        _appendedLsn = _lastLsn;
        _durableLsn = _lastLsn;
    }

    public string FilePath => _store.FilePath;

    internal int DurableFlushCount => _store.DurableFlushCount;

    /// <summary>The highest LSN assigned so far.</summary>
    public long LastLsn
    {
        get
        {
            lock (_appendGate)
            {
                return _lastLsn;
            }
        }
    }

    /// <summary>
    /// Raises the LSN counter to at least <paramref name="lsn"/>. Callers pass the highest LSN already
    /// persisted elsewhere (table manifests), because a fully pruned log no longer records it.
    /// </summary>
    public void EnsureLsnAtLeast(long lsn)
    {
        lock (_appendGate)
        {
            if (lsn > _lastLsn)
            {
                _lastLsn = lsn;
                Volatile.Write(ref _appendedLsn, Math.Max(Volatile.Read(ref _appendedLsn), lsn));
                Volatile.Write(ref _durableLsn, Math.Max(Volatile.Read(ref _durableLsn), lsn));
            }
        }
    }

    /// <summary>
    /// Appends <paramref name="batch"/> as one frame and returns its ticket. The frame has reached the OS
    /// when this returns; call <see cref="LsmCommitTicket.Wait"/> (outside any lock) before acknowledging
    /// the write in strict mode.
    /// </summary>
    public LsmCommitTicket Append(LsmCommitBatch batch)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(batch);
        if (batch.IsEmpty)
        {
            throw new ArgumentException("A commit batch must contain at least one operation.", nameof(batch));
        }

        DataVo.Core.Compat.ThrowHelper.ThrowIfDisposed(_disposed, this);
        int payloadLength = LsmCommitBatchCodec.MeasureSize(batch);
        int frameLength = checked(WalAppender.FrameHeaderSize + payloadLength);
        byte[] rented = ArrayPool<byte>.Shared.Rent(frameLength);
        try
        {
            Span<byte> frame = rented.AsSpan(0, frameLength);
            LsmCommitBatchCodec.Write(frame[WalAppender.FrameHeaderSize..], batch);

            long lsn;
            lock (_appendGate)
            {
                // The LSN is consumed only once the frame reached the OS: a failed write must not leave a
                // gap that later commits would wait on forever.
                lsn = checked(_lastLsn + 1);
                WriteHeader(frame, lsn, batch.Count);
                try
                {
                    _store.AppendFrameBytes(frame, flushToDisk: false);
                }
                catch
                {
                    // A partial frame would hide every later frame from recovery; cut it off.
                    _store.TruncateThroughLsn(0);
                    throw;
                }

                _lastLsn = lsn;
                Volatile.Write(ref _appendedLsn, lsn);
            }

            return new LsmCommitTicket(_durabilityMode == LsmWalDurabilityMode.StrictFsync ? this : null, lsn);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>Blocks until every frame up to <paramref name="lsn"/> is covered by an fsync.</summary>
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
                // Capture the appended watermark before the fsync: the fsync covers at least that much,
                // and followers that append meanwhile are covered by the next leader in one flush.
                long target = Volatile.Read(ref _appendedLsn);
                _store.FlushToDiskConcurrent();
                Volatile.Write(ref _durableLsn, target);
            }
        }
    }

    /// <summary>
    /// Returns every frame of the valid prefix in LSN order. A frame whose checksum is valid but whose
    /// payload does not decode means a format or software bug, so it fails loudly instead of being skipped.
    /// </summary>
    public IReadOnlyList<LsmCommitFrame> ReadAll()
    {
        var frames = new List<LsmCommitFrame>();
        foreach (WalFrameRecord record in _store.ReadBinaryFrames())
        {
            if (record.Header.OpType != WalFrameOperationType.CommitBatch
                || !LsmCommitBatchCodec.TryRead(record.Payload, out List<LsmCommitOperation> operations)
                || operations.Count != record.Header.RowId)
            {
                throw new InvalidDataException(
                    $"Commit log '{FilePath}' has a checksummed frame at LSN {record.Header.Lsn} that is not a valid commit batch.");
            }

            frames.Add(new LsmCommitFrame(record.Header.Lsn, operations));
        }

        return frames;
    }

    /// <summary>Drops every frame with an LSN at or below <paramref name="lsn"/>, whose effects are durable elsewhere.</summary>
    public void PruneThrough(long lsn) => _store.TruncateThroughLsn(lsn);

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        if (_durabilityMode == LsmWalDurabilityMode.StrictFsync && Volatile.Read(ref _appendedLsn) > Volatile.Read(ref _durableLsn))
        {
            _store.FlushToDisk();
        }

        _store.ReleaseFileHandle();
    }

    // Same 32-byte header as the other binary WAL frames; the RowId slot carries the operation count.
    private static void WriteHeader(Span<byte> frame, long lsn, int operationCount)
    {
        Span<byte> header = frame[..WalAppender.FrameHeaderSize];
        header.Clear();
        BinaryPrimitives.WriteInt32LittleEndian(header, frame.Length);
        BinaryPrimitives.WriteInt64LittleEndian(header[8..], lsn);
        header[16] = (byte)WalFrameOperationType.CommitBatch;
        BinaryPrimitives.WriteInt64LittleEndian(header[20..], operationCount);
        BinaryPrimitives.WriteInt32LittleEndian(header[28..], 0);
        uint checksum = WalCrc32C.HashToUInt32(frame[8..]);
        BinaryPrimitives.WriteUInt32LittleEndian(header[4..], checksum);
    }
}

/// <summary>Durability handle for one appended commit batch.</summary>
internal readonly struct LsmCommitTicket
{
    private readonly LsmCommitLog? _log;

    internal LsmCommitTicket(LsmCommitLog? log, long lsn)
    {
        _log = log;
        Lsn = lsn;
    }

    /// <summary>The batch's LSN, which is also the sequence number of every mutation in it.</summary>
    public long Lsn { get; }

    /// <summary>Waits until the batch is durable (strict mode); returns immediately in relaxed mode.</summary>
    public void Wait() => _log?.EnsureDurableThrough(Lsn);
}
