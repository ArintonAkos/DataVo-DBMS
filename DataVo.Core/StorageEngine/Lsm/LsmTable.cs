using System.Buffers;
using System.Globalization;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>Result of flushing one MemTable generation to an SSTable file.</summary>
public readonly record struct LsmFlushResult(
    LsmTableFileMetadata Metadata,
    long ByteCount,
    string FilePath);

internal readonly record struct LsmBatchRowPutEntry(long RowId, ulong Seqno, byte[] Value);

/// <summary>
/// Coordinates the active MemTable, a queue of frozen (immutable) MemTable generations awaiting
/// background flush, SSTable/manifest publication, and background Level-0 compaction.
/// <para>
/// Write path: mutations append to the current WAL segment and the active MemTable under the write
/// gate. When the active MemTable crosses the size threshold it is frozen, a fresh MemTable and WAL
/// segment are swapped in atomically, and the frozen generation is handed to a single background
/// worker — the crossing writer returns without performing flush I/O. Writers block on the flush
/// pipeline only when <see cref="MaxPendingFrozenGenerations"/> generations are already queued
/// (backpressure), never for routine flush I/O.
/// </para>
/// <para>
/// Each generation owns its own WAL segment; the segment is deleted only after the generation's
/// SSTable and manifest edit are durable, so a crash at any point recovers every acknowledged write
/// by replaying the surviving segments in order.
/// </para>
/// </summary>
public sealed class LsmTable : IDisposable
{
    internal const int DefaultMaxCompactionLevel = 7;

    /// <summary>
    /// Active MemTable size that triggers rotation into the background flush pipeline. Size-triggered
    /// (RocksDB-style) so the flush count is proportional to data volume, not to probe frequency.
    /// Internal-settable so tests can exercise rotation without writing 32 MB.
    /// </summary>
    internal long FlushThresholdBytes { get; set; } = 32L << 20;

    /// <summary>Frozen generations allowed in the flush queue before writers backpressure-block.</summary>
    internal int MaxPendingFrozenGenerations { get; set; } = 2;

    /// <summary>Level-0 live-file count that triggers a background merge into Level 1.</summary>
    internal int Level0CompactionThreshold { get; set; } = 4;

    /// <summary>Live-file count that triggers background compaction for non-zero levels.</summary>
    internal int LevelCompactionThreshold { get; set; } = 4;

    /// <summary>Highest persisted LSM level. Compaction into this level can drop tombstones.</summary>
    internal int MaxCompactionLevel { get; set; } = DefaultMaxCompactionLevel;

    /// <summary>File registry used by background compaction; compaction is disabled while null.</summary>
    internal LsmFileRegistry? CompactionRegistry { get; set; }

    private sealed record FrozenGeneration(MemTable Table, LsmWalWriter? Wal);

    private readonly object _gate = new();
    private readonly object _ioGate = new();
    private readonly string _tableDirectory;
    private readonly LsmManifest _manifest;
    private readonly Action<LsmVersionEdit> _applyEdit;
    private readonly Action<LsmCrashPoint>? _crashHook;
    private readonly string? _walBasePath;
    private readonly LsmWalDurabilityMode _walDurabilityMode;
    private readonly Queue<FrozenGeneration> _frozenQueue = new();
    private readonly List<string> _recoveredWalPaths = [];
    private LsmWalWriter? _currentWal;
    private long _walSegmentSeq;
    private MemTable _active = new();
    private Task _flushWorker = Task.CompletedTask;
    private bool _workerBusy;
    private Exception? _backgroundError;
    private bool _disposed;

    /// <summary>Creates an LSM table rooted at <paramref name="tableDirectory"/> and backed by <paramref name="manifest"/>.</summary>
    public LsmTable(string tableDirectory, LsmManifest manifest)
        : this(tableDirectory, manifest, walPath: null, LsmWalDurabilityMode.StrictFsync, applyEdit: null)
    {
    }

    /// <summary>
    /// Creates an LSM table whose MemTable generations are covered by WAL segments derived from
    /// <paramref name="walPath"/>. Strict fsync is the default production durability mode.
    /// </summary>
    public LsmTable(
        string tableDirectory,
        LsmManifest manifest,
        string walPath,
        LsmWalDurabilityMode durabilityMode = LsmWalDurabilityMode.StrictFsync)
        : this(tableDirectory, manifest, walPath, durabilityMode, applyEdit: null)
    {
    }

    private LsmTable(
        string tableDirectory,
        LsmManifest manifest,
        string? walPath,
        LsmWalDurabilityMode durabilityMode,
        Action<LsmVersionEdit>? applyEdit,
        Action<LsmCrashPoint>? crashHook = null)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNullOrWhiteSpace(tableDirectory);
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(manifest);

        _tableDirectory = tableDirectory;
        _manifest = manifest;
        _crashHook = crashHook;
        _walBasePath = walPath;
        _walDurabilityMode = durabilityMode;
        _applyEdit = applyEdit ?? manifest.ApplyEdit;
        Directory.CreateDirectory(_tableDirectory);
        RecoverWalSegments();
    }

    internal static LsmTable CreateForTesting(
        string tableDirectory,
        LsmManifest manifest,
        Action<LsmVersionEdit> applyEdit) =>
        new(tableDirectory, manifest, walPath: null, LsmWalDurabilityMode.StrictFsync, applyEdit);

    internal static LsmTable CreateForTesting(
        string tableDirectory,
        LsmManifest manifest,
        string walPath,
        LsmWalDurabilityMode durabilityMode,
        Action<LsmCrashPoint> crashHook) =>
        new(tableDirectory, manifest, walPath, durabilityMode, applyEdit: null, crashHook);

    /// <summary>Number of version entries currently buffered in the active mutable MemTable.</summary>
    public int ActiveCount
    {
        get
        {
            lock (_gate)
            {
                DataVo.Core.Compat.ThrowHelper.ThrowIfDisposed(_disposed, this);
                return _active.Count;
            }
        }
    }

    /// <summary>
    /// Adds a value version to the active MemTable. Synchronously durable in strict mode: the call
    /// returns only after the WAL frame is covered by a (possibly shared, group-commit) fsync.
    /// </summary>
    public void Put(ReadOnlySpan<byte> userKey, ulong seqno, ReadOnlySpan<byte> value)
    {
        PutDeferDurability(userKey, seqno, value).Wait();
    }

    /// <summary>
    /// Adds a value version and returns a durability ticket instead of waiting: the caller must
    /// invoke <see cref="LsmWalDurabilityTicket.Wait"/> OUTSIDE its own locks before acknowledging
    /// the operation, which lets concurrent strict-mode writers share one group-commit fsync.
    /// </summary>
    internal LsmWalDurabilityTicket PutDeferDurability(ReadOnlySpan<byte> userKey, ulong seqno, ReadOnlySpan<byte> value)
    {
        lock (_gate)
        {
            ThrowIfUnusableNoLock();
            LsmWalDurabilityTicket ticket =
                _currentWal?.AppendMutationBuffered(userKey, seqno, LsmValueType.Put, value)
                ?? LsmWalDurabilityTicket.None;
            _active.Put(userKey, seqno, LsmValueType.Put, value);
            RotateIfOversizedNoLock();
            return ticket;
        }
    }

    internal LsmWalDurabilityTicket PutRowIdBatch(ReadOnlySpan<LsmBatchRowPutEntry> entries)
    {
        lock (_gate)
        {
            ThrowIfUnusableNoLock();
            if (entries.IsEmpty)
            {
                return LsmWalDurabilityTicket.None;
            }

            LsmWalDurabilityTicket ticket =
                _currentWal?.AppendRowIdPutMutationsBatch(entries) ?? LsmWalDurabilityTicket.None;

            Span<byte> userKey = stackalloc byte[sizeof(long)];
            foreach (LsmBatchRowPutEntry entry in entries)
            {
                InternalKey.EncodeInt64UserKey(userKey, entry.RowId);
                _active.Put(userKey, entry.Seqno, LsmValueType.Put, entry.Value);
            }

            RotateIfOversizedNoLock();
            return ticket;
        }
    }

    /// <summary>Adds a tombstone version to the active MemTable (synchronously durable in strict mode).</summary>
    public void Delete(ReadOnlySpan<byte> userKey, ulong seqno)
    {
        DeleteDeferDurability(userKey, seqno).Wait();
    }

    internal LsmWalDurabilityTicket DeleteDeferDurability(ReadOnlySpan<byte> userKey, ulong seqno)
    {
        lock (_gate)
        {
            ThrowIfUnusableNoLock();
            LsmWalDurabilityTicket ticket =
                _currentWal?.AppendMutationBuffered(userKey, seqno, LsmValueType.Deletion, ReadOnlySpan<byte>.Empty)
                ?? LsmWalDurabilityTicket.None;
            _active.Delete(userKey, seqno);
            RotateIfOversizedNoLock();
            return ticket;
        }
    }

    /// <summary>
    /// Synchronously drains the background flush queue, then flushes the active MemTable inline:
    /// serializes it to a Level-0 SSTable, publishes it atomically, registers it in the manifest,
    /// installs a fresh active table, and clears the covered WAL state. Empty active tables are a
    /// no-op. Used by explicit maintenance (compaction, close) and tests; routine flushes ride the
    /// background pipeline instead.
    /// </summary>
    public LsmFlushResult? FlushActiveMemTable()
    {
        LsmFlushResult? result;
        lock (_gate)
        {
            DataVo.Core.Compat.ThrowHelper.ThrowIfDisposed(_disposed, this);
            WaitForFlushPipelineNoLock();
            ThrowIfUnusableNoLock();

            if (_active.Count == 0)
            {
                CleanupRecoveredWalsNoLock();
                return null;
            }

            result = FlushMemTableToSstable(_active);
            MemTable flushed = _active;
            flushed.Freeze();
            _active = new MemTable();
            flushed.Dispose();
            _currentWal?.Clear();
            CleanupRecoveredWalsNoLock();
        }

        ScheduleCompactionCheck();
        return result;
    }

    /// <summary>
    /// Serializes maintenance operations (background flush, background compaction, explicit
    /// compaction) that mutate SSTable files and the manifest.
    /// </summary>
    internal object MaintenanceGate => _ioGate;

    /// <summary>Physical fsyncs issued by the CURRENT WAL segment (diagnostics/tests).</summary>
    internal int WalDurableFlushCount
    {
        get
        {
            lock (_gate)
            {
                return _currentWal?.DurableFlushCount ?? 0;
            }
        }
    }

    /// <summary>Releases the current active MemTable generation after draining pending flushes.</summary>
    public void Dispose()
    {
        MemTable active;
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            // Drain the pipeline so no background flush touches state after disposal. A background
            // error at this point is swallowed: Dispose must not throw, and every acknowledged
            // write remains recoverable from the surviving WAL segments.
            WaitForFlushPipelineNoLock();

            _disposed = true;
            active = _active;
            _active = null!;
        }

        active.Dispose();
    }

    private void RotateIfOversizedNoLock()
    {
        if (_active.ApproximateBytes < FlushThresholdBytes)
        {
            return;
        }

        // Backpressure: only when the pipeline is genuinely overwhelmed does a writer wait.
        while (_frozenQueue.Count >= MaxPendingFrozenGenerations)
        {
            Monitor.Wait(_gate);
            ThrowIfUnusableNoLock();
        }

        MemTable frozen = _active;
        frozen.Freeze();
        LsmWalWriter? frozenWal = _currentWal;

        _active = new MemTable();
        // The frozen generation keeps its own segment file (the base path for the first
        // generation, a numbered path otherwise); the new active generation always gets a fresh
        // numbered segment. No file is ever renamed, so in-flight group-commit tickets against the
        // frozen segment keep fsyncing the file that actually holds their frames.
        _currentWal = _walBasePath is null ? null : CreateNextWalSegmentNoLock();

        _frozenQueue.Enqueue(new FrozenGeneration(frozen, frozenWal));
        _flushWorker = _flushWorker.ContinueWith(
            _ => DrainFrozenQueue(),
            CancellationToken.None,
            TaskContinuationOptions.None,
            TaskScheduler.Default);
    }

    private void DrainFrozenQueue()
    {
        while (true)
        {
            FrozenGeneration generation;
            lock (_gate)
            {
                if (_backgroundError is not null || _frozenQueue.Count == 0)
                {
                    _workerBusy = false;
                    Monitor.PulseAll(_gate);
                    return;
                }

                generation = _frozenQueue.Dequeue();
                _workerBusy = true;
            }

            try
            {
                FlushGeneration(generation);
            }
            catch (Exception ex)
            {
                lock (_gate)
                {
                    _backgroundError = ex;
                    _workerBusy = false;
                    Monitor.PulseAll(_gate);
                }

                return;
            }

            MaybeCompactLevels();

            lock (_gate)
            {
                _workerBusy = false;
                Monitor.PulseAll(_gate);
            }
        }
    }

    private void FlushGeneration(FrozenGeneration generation)
    {
        // The read lease pins the frozen generation's arena slabs while the SSTable image is
        // serialized from them; without it, a concurrent Dispose could return slabs to the shared
        // pool while this thread still reads their spans.
        using (ArenaLease lease = generation.Table.AcquireReadLease())
        {
            if (generation.Table.Count > 0)
            {
                FlushMemTableToSstable(generation.Table);
            }
        }

        generation.Table.Dispose();
        // The generation's WAL segment is deleted only now, after its SSTable and manifest edit are
        // durable: a crash before this point replays the segment; after it, the SSTable is truth.
        generation.Wal?.Clear();
    }

    private LsmFlushResult FlushMemTableToSstable(MemTable memTable)
    {
        lock (_ioGate)
        {
            (byte[] smallest, byte[] largest) = ComputeBounds(memTable);
            byte[] imageBuffer = SsTableWriter.WriteRented(memTable, out int imageLength);
            try
            {
                long fileNumber = _manifest.AllocateFileNumber();
                string fileName = $"{fileNumber:D6}.sst";
                string filePath = Path.Combine(_tableDirectory, fileName);
                WriteSstableAtomically(filePath, imageBuffer, imageLength, _crashHook);

                var metadata = new LsmTableFileMetadata(
                    fileNumber,
                    level: 0,
                    smallest,
                    largest,
                    imageLength,
                    fileName);

                var edit = new LsmVersionEdit();
                edit.AddFile(metadata);
                try
                {
                    _manifest.CrashHook = _crashHook;
                    _applyEdit(edit);
                }
                catch (LsmCrashSimulationException)
                {
                    throw;
                }
                catch
                {
                    DeleteFileIfExists(filePath);
                    throw;
                }
                finally
                {
                    _manifest.CrashHook = null;
                }

                return new LsmFlushResult(metadata, imageLength, filePath);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(imageBuffer);
            }
        }
    }

    internal void CompactAllLevelsForMaintenance()
    {
        LsmFileRegistry? registry = CompactionRegistry;
        if (registry is null)
        {
            return;
        }

        lock (_ioGate)
        {
            CompactAllLevels(registry);
        }
    }

    private void MaybeCompactLevels()
    {
        LsmFileRegistry? registry = CompactionRegistry;
        if (registry is null)
        {
            return;
        }

        try
        {
            lock (_ioGate)
            {
                CompactEligibleLevels(registry);
            }
        }
        catch (Exception ex)
        {
            lock (_gate)
            {
                _backgroundError ??= ex;
            }
        }
    }

    private void ScheduleCompactionCheck()
    {
        lock (_gate)
        {
            if (_disposed || CompactionRegistry is null)
            {
                return;
            }

            _flushWorker = _flushWorker.ContinueWith(
                _ => RunScheduledCompactionCheck(),
                CancellationToken.None,
                TaskContinuationOptions.None,
                TaskScheduler.Default);
        }
    }

    private void RunScheduledCompactionCheck()
    {
        lock (_gate)
        {
            if (_disposed || _backgroundError is not null)
            {
                return;
            }

            _workerBusy = true;
        }

        try
        {
            MaybeCompactLevels();
        }
        finally
        {
            lock (_gate)
            {
                _workerBusy = false;
                Monitor.PulseAll(_gate);
            }
        }
    }

    private void CompactEligibleLevels(LsmFileRegistry registry)
    {
        int maxLevel = ValidatedMaxCompactionLevel();
        var compactor = new LsmCompactor(_tableDirectory, _manifest, registry);

        bool madeProgress;
        do
        {
            madeProgress = false;
            for (int sourceLevel = 0; sourceLevel < maxLevel; sourceLevel++)
            {
                int threshold = sourceLevel == 0 ? Level0CompactionThreshold : LevelCompactionThreshold;
                if (threshold <= 0)
                {
                    throw new InvalidOperationException("LSM compaction thresholds must be positive.");
                }

                if (_manifest.GetLiveFiles(sourceLevel).Count < threshold)
                {
                    continue;
                }

                int targetLevel = sourceLevel + 1;
                compactor.CompactLevel(
                    sourceLevel,
                    targetLevel,
                    dropTombstonesAtBottomLevel: targetLevel == maxLevel);
                madeProgress = true;
            }
        }
        while (madeProgress);
    }

    private void CompactAllLevels(LsmFileRegistry registry)
    {
        int maxLevel = ValidatedMaxCompactionLevel();
        var compactor = new LsmCompactor(_tableDirectory, _manifest, registry);

        for (int sourceLevel = 0; sourceLevel < maxLevel; sourceLevel++)
        {
            while (_manifest.GetLiveFiles(sourceLevel).Count > 0)
            {
                int targetLevel = sourceLevel + 1;
                compactor.CompactLevel(
                    sourceLevel,
                    targetLevel,
                    dropTombstonesAtBottomLevel: targetLevel == maxLevel);
            }
        }
    }

    private int ValidatedMaxCompactionLevel()
    {
        if (MaxCompactionLevel < 1)
        {
            throw new InvalidOperationException("LSM max compaction level must be at least 1.");
        }

        return MaxCompactionLevel;
    }

    private void WaitForFlushPipelineNoLock()
    {
        while ((_frozenQueue.Count > 0 || _workerBusy) && _backgroundError is null)
        {
            Monitor.Wait(_gate);
        }
    }

    private void ThrowIfUnusableNoLock()
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfDisposed(_disposed, this);
        if (_backgroundError is not null)
        {
            throw new InvalidOperationException(
                "The LSM table is fail-stopped: a background flush or compaction failed. All acknowledged writes remain recoverable from the WAL segments.",
                _backgroundError);
        }
    }

    private LsmWalWriter CreateNextWalSegmentNoLock()
    {
        _walSegmentSeq++;
        return new LsmWalWriter(WalSegmentPath(_walSegmentSeq), _walDurabilityMode);
    }

    private string WalSegmentPath(long seq)
    {
        string directory = Path.GetDirectoryName(_walBasePath!)!;
        string stem = Path.GetFileNameWithoutExtension(_walBasePath!);
        return Path.Combine(directory, $"{stem}.{seq:D6}.wal");
    }

    private void RecoverWalSegments()
    {
        if (_walBasePath is null)
        {
            _currentWal = null;
            return;
        }

        string directory = Path.GetDirectoryName(_walBasePath)!;
        string stem = Path.GetFileNameWithoutExtension(_walBasePath);
        var segments = new List<(long Seq, string Path)>();

        // Legacy single-file layout: the base path itself, replayed before any numbered segment.
        if (File.Exists(_walBasePath))
        {
            segments.Add((-1L, _walBasePath));
        }

        var pattern = new Regex(
            "^" + Regex.Escape(stem) + @"\.(\d{6,})\.wal$",
            RegexOptions.CultureInvariant);
        if (Directory.Exists(directory))
        {
            foreach (string path in Directory.GetFiles(directory, $"{stem}.*.wal"))
            {
                Match match = pattern.Match(Path.GetFileName(path));
                if (match.Success)
                {
                    segments.Add((long.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture), path));
                }
            }
        }

        segments.Sort(static (left, right) => left.Seq.CompareTo(right.Seq));
        long maxSeq = 0;
        foreach ((long seq, string path) in segments)
        {
            var replayWriter = new LsmWalWriter(path, _walDurabilityMode);
            replayWriter.ReplayInto(_active);
            maxSeq = Math.Max(maxSeq, seq);
            if (seq >= 0)
            {
                // Numbered (rotated) segments are deleted once the recovered data is flushed. The
                // base path is not tracked here: it becomes the live segment again below, and its
                // replayed frames are cleared by the current writer's own post-flush Clear.
                _recoveredWalPaths.Add(path);
            }
        }

        // The live WAL always sits at the base path; rotated frozen generations get numbered paths.
        _walSegmentSeq = maxSeq;
        _currentWal = new LsmWalWriter(_walBasePath, _walDurabilityMode);

        if (_active.Count == 0)
        {
            // Nothing recoverable in the surviving rotated segments; deleting them loses nothing.
            CleanupRecoveredWalsNoLock();
        }
    }

    private void CleanupRecoveredWalsNoLock()
    {
        foreach (string path in _recoveredWalPaths)
        {
            DeleteFileIfExists(path);
        }

        _recoveredWalPaths.Clear();
    }

    private static (byte[] Smallest, byte[] Largest) ComputeBounds(MemTable memTable)
    {
        if (!memTable.TryGetInternalKeyBounds(out byte[] smallest, out byte[] largest))
        {
            throw new InvalidOperationException("Cannot flush an empty MemTable.");
        }

        return (smallest, largest);
    }

    private static void WriteSstableAtomically(
        string filePath,
        byte[] bytes,
        int length,
        Action<LsmCrashPoint>? crashHook)
    {
        LsmDurableFileOperations.WriteFileAtomically(
            filePath,
            overwrite: false,
            stream => stream.Write(bytes, 0, length),
            crashHook,
            LsmCrashPoint.AfterSstableTempFileFsyncBeforeRename,
            LsmCrashPoint.AfterSstableRenameBeforeDirectoryFsync,
            LsmCrashPoint.AfterSstableDirectoryFsyncBeforeManifest);
    }

    private static void DeleteFileIfExists(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }
}
