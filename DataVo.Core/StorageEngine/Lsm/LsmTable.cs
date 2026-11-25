using System.Buffers;
using System.Globalization;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using DataVo.Core.StorageEngine.Lsm.V2;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>Result of flushing one MemTable generation to an SSTable file.</summary>
public readonly record struct LsmFlushResult(
    LsmTableFileMetadata Metadata,
    long ByteCount,
    string FilePath);

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

    /// <summary>
    /// The columns every row-keyed SSTable written from now on (by a flush or a compaction) carries a second
    /// time as a columnar segment, or null to write none.
    /// </summary>
    internal DataVo.Core.Indexing.Columnar.ColumnarSpec? ColumnarSpec
    {
        get => Volatile.Read(ref _columnarSpec);
        set => Volatile.Write(ref _columnarSpec, value);
    }

    private DataVo.Core.Indexing.Columnar.ColumnarSpec? _columnarSpec;

    /// <summary>File registry used by background compaction; compaction is disabled while null.</summary>
    internal LsmFileRegistry? CompactionRegistry { get; set; }

    /// <summary>
    /// Called with a MemTable's highest sequence number before it is written as an SSTable. With a shared
    /// commit log the owner makes that log durable through it first, so an SSTable never holds part of a
    /// batch whose commit frame could still be lost.
    /// </summary>
    internal Action<ulong>? BeforeFlushDurabilityBarrier { get; set; }

    /// <summary>Called (outside this table's locks) after a MemTable has been durably flushed.</summary>
    internal Action? AfterFlush { get; set; }

    /// <summary>
    /// SSTable format written on flush and compaction: <see cref="LsmSstableFormat.Keyed"/> (the default, any
    /// byte keys) or <see cref="LsmSstableFormat.RowKeyed"/>, which requires every user key to be a 16-byte row
    /// key or the table metadata key.
    /// </summary>
    internal LsmSstableFormat Format { get; set; } = LsmSstableFormat.Keyed;

    /// <summary>
    /// Bloom setting of keyed files: 0 (the default) filters on the whole user key, <see cref="LsmIndexKeyCodec.BloomSuffix"/>
    /// on an index entry's length-prefixed key (see <see cref="SsTableKeyedFormat.BloomKeyLength"/>).
    /// </summary>
    internal int BloomSuffixLength { get; set; }

    /// <summary>How compaction reads v2 inputs.</summary>
    internal LsmReadMode ReadMode { get; set; }

    /// <summary>The engine's block cache for positional reads, or null.</summary>
    internal LsmBlockCache? BlockCache { get; set; }

    /// <summary>
    /// Called on the flushing thread once a MemTable's SSTable is durable and in the manifest, while the
    /// MemTable is still readable: the owner can drop row versions that now live in the file.
    /// </summary>
    internal Action<MemTable>? AfterSstablePublished { get; set; }

    private sealed record FrozenGeneration(MemTable Table, LsmWalWriter? Wal);

    private readonly object _gate = new();
    private int _frameHolds; // guarded by _gate
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
    private MemTable? _flushing; // frozen generation dequeued by the worker but not yet durable
    private Task _flushWorker = Task.CompletedTask;
    private bool _workerBusy;
    private int _pendingCompactionChecks;
    private Exception? _backgroundError;
    private bool _stopping;
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
            while (_frameHolds > 0)
            {
                Monitor.Wait(_gate); // never flush part of a commit frame (see BeginFrame)
            }

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
        AfterFlush?.Invoke();
        return result;
    }

    /// <summary>
    /// Smallest sequence number that is not yet durable in an SSTable (active, queued, or in-flight
    /// generations), or <see langword="null"/> when everything this table holds is flushed.
    /// </summary>
    internal ulong? OldestUnflushedSequence
    {
        get
        {
            lock (_gate)
            {
                ulong? oldest = null;
                void Consider(MemTable? table)
                {
                    if (table?.MinSequence is ulong min && (oldest is null || min < oldest))
                    {
                        oldest = min;
                    }
                }

                Consider(_active);
                Consider(_flushing);
                foreach (FrozenGeneration generation in _frozenQueue)
                {
                    Consider(generation.Table);
                }

                return oldest;
            }
        }
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

    /// <summary>
    /// Waits until all queued frozen MemTable flushes and scheduled background compaction checks finish.
    /// </summary>
    /// <example>
    /// <code>
    /// table.FlushActiveMemTable();
    /// table.WaitForBackgroundWork();
    /// </code>
    /// </example>
    internal void WaitForBackgroundWork()
    {
        lock (_gate)
        {
            DataVo.Core.Compat.ThrowHelper.ThrowIfDisposed(_disposed, this);
            WaitForFlushPipelineNoLock(waitForScheduledCompactions: true);
            ThrowIfUnusableNoLock();
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

            _stopping = true;

            // Drain pending flushes and any in-flight compaction so no background work touches state
            // after disposal; scheduled compaction checks that have not started exit early on _stopping.
            // A background error at this point is swallowed: Dispose must not throw, and every
            // acknowledged write remains recoverable from the surviving WAL segments.
            WaitForFlushPipelineNoLock(waitForScheduledCompactions: false);

            _disposed = true;
            active = _active;
            _active = null!;
        }

        active.Dispose();
    }

    /// <summary>
    /// Keeps the active MemTable from rotating or being flushed until <see cref="EndFrame"/>. Recovery
    /// replays or skips a commit frame whole, by its LSN, so a frame must land in one MemTable: a file
    /// flushed halfway through a frame would claim the frame's LSN while the rest was only in memory,
    /// and a crash then lost those rows.
    /// </summary>
    internal void BeginFrame()
    {
        lock (_gate)
        {
            _frameHolds++;
        }
    }

    /// <summary>Ends a <see cref="BeginFrame"/> hold and rotates now if the MemTable outgrew its threshold.</summary>
    internal void EndFrame()
    {
        lock (_gate)
        {
            _frameHolds--;
            Monitor.PulseAll(_gate);
            if (_frameHolds == 0 && !_disposed)
            {
                RotateIfOversizedNoLock();
            }
        }
    }

    private void RotateIfOversizedNoLock()
    {
        if (_frameHolds > 0 || _active.ApproximateBytes < FlushThresholdBytes)
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
                _flushing = generation.Table;
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

            lock (_gate)
            {
                _flushing = null;
            }

            AfterFlush?.Invoke();

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
        if (BeforeFlushDurabilityBarrier is { } barrier && memTable.MaxSequence is ulong maxSequence)
        {
            barrier(maxSequence);
        }

        if (Format == LsmSstableFormat.RowKeyed)
        {
            return FlushMemTableToV2Sstable(memTable);
        }

        return FlushMemTableToKeyedSstable(memTable);
    }

    /// <summary>
    /// Streams <paramref name="memTable"/> into a v2 SSTable block by block (no whole-file image), publishes
    /// it through the file registry's version gate, then lets the owner evict the flushed versions.
    /// </summary>
    private readonly record struct RowBounds(
        bool HasRows,
        RowKey FirstKey,
        ulong FirstSequence,
        LsmValueType FirstType,
        RowKey LastKey,
        ulong LastSequence,
        LsmValueType LastType);

    /// <summary>
    /// Streams every entry of <paramref name="memTable"/> into <paramref name="writer"/> and returns the first
    /// and last row versions. Compiled fully optimized up front: a flush runs a few times per process, so
    /// tiered compilation would otherwise run it mostly as unoptimized code (about 3x slower).
    /// </summary>
    [System.Runtime.CompilerServices.MethodImpl(DataVo.Core.Compat.MethodImplCompat.AggressiveOptimization)]
    private static RowBounds WriteRows(SsTableV2Writer writer, MemTable memTable)
    {
        bool hasRows = false;
        RowKey firstKey = default;
        ulong firstSequence = 0;
        LsmValueType firstType = LsmValueType.Put;
        RowKey lastKey = default;
        ulong lastSequence = 0;
        LsmValueType lastType = LsmValueType.Put;
        foreach (MemTableEntry entry in memTable)
        {
            LsmRowKeyCodec.AddInternalEntry(writer, entry.InternalKey, entry.Value);
            ReadOnlySpan<byte> userKey = InternalKey.UserKey(entry.InternalKey);
            if (!LsmRowKeyCodec.IsRowKey(userKey))
            {
                continue;
            }

            lastKey = LsmRowKeyCodec.DecodeRowKey(userKey);
            lastSequence = InternalKey.Sequence(entry.InternalKey);
            lastType = InternalKey.ValueType(entry.InternalKey);
            if (!hasRows)
            {
                hasRows = true;
                firstKey = lastKey;
                firstSequence = lastSequence;
                firstType = lastType;
            }
        }

        return new RowBounds(hasRows, firstKey, firstSequence, firstType, lastKey, lastSequence, lastType);
    }

    private LsmFlushResult FlushMemTableToV2Sstable(MemTable memTable)
    {
        lock (_ioGate)
        {
            long fileNumber = _manifest.AllocateFileNumber();
            string fileName = $"{fileNumber:D6}.sst";
            string filePath = Path.Combine(_tableDirectory, fileName);

            long length = 0;
            bool hasRows = false;
            RowKey firstRowKey = default;
            ulong firstSequence = 0;
            LsmValueType firstType = LsmValueType.Put;
            RowKey lastRowKey = default;
            ulong lastSequence = 0;
            LsmValueType lastType = LsmValueType.Put;

            LsmDurableFileOperations.WriteFileAtomically(
                filePath,
                overwrite: false,
                stream =>
                {
                    using var writer = new SsTableV2Writer(stream, alignment: 0, memTable.Count, columnar: ColumnarSpec);
                    RowBounds bounds = WriteRows(writer, memTable);
                    if (!bounds.HasRows)
                    {
                        throw new InvalidOperationException("A MemTable without row versions cannot be flushed to a row-keyed SSTable.");
                    }

                    (hasRows, firstRowKey, firstSequence, firstType) = (true, bounds.FirstKey, bounds.FirstSequence, bounds.FirstType);
                    (lastRowKey, lastSequence, lastType) = (bounds.LastKey, bounds.LastSequence, bounds.LastType);
                    length = writer.Finish();
                },
                _crashHook,
                LsmCrashPoint.AfterSstableTempFileFsyncBeforeRename,
                LsmCrashPoint.AfterSstableRenameBeforeDirectoryFsync,
                LsmCrashPoint.AfterSstableDirectoryFsyncBeforeManifest);

            var metadata = new LsmTableFileMetadata(
                fileNumber,
                level: 0,
                LsmRowKeyCodec.RowInternalKey(firstRowKey, firstSequence, firstType),
                LsmRowKeyCodec.RowInternalKey(lastRowKey, lastSequence, lastType),
                length,
                fileName);

            var edit = new LsmVersionEdit();
            edit.AddFile(metadata);
            try
            {
                _manifest.CrashHook = _crashHook;
                if (CompactionRegistry is { } registry)
                {
                    // Under the version gate: readers build their file sets from the manifest there too.
                    registry.ExecuteVersionEdit(() =>
                    {
                        _applyEdit(edit);
                        registry.Register(metadata);
                        return true;
                    });
                }
                else
                {
                    _applyEdit(edit);
                }
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

            AfterSstablePublished?.Invoke(memTable);
            return new LsmFlushResult(metadata, length, filePath);
        }
    }

    /// <summary>Streams <paramref name="memTable"/> into a keyed (v3) SSTable; see <see cref="FlushMemTableToV2Sstable"/>.</summary>
    private LsmFlushResult FlushMemTableToKeyedSstable(MemTable memTable)
    {
        lock (_ioGate)
        {
            (byte[] smallest, byte[] largest) = ComputeBounds(memTable);
            long fileNumber = _manifest.AllocateFileNumber();
            string fileName = $"{fileNumber:D6}.sst";
            string filePath = Path.Combine(_tableDirectory, fileName);
            long length = 0;
            LsmDurableFileOperations.WriteFileAtomically(
                filePath,
                overwrite: false,
                stream =>
                {
                    using var writer = new SsTableKeyedWriter(stream, BloomSuffixLength);
                    foreach (MemTableEntry entry in memTable)
                    {
                        writer.Add(
                            InternalKey.UserKey(entry.InternalKey),
                            InternalKey.Sequence(entry.InternalKey),
                            InternalKey.ValueType(entry.InternalKey),
                            entry.Value);
                    }

                    length = writer.Finish();
                },
                _crashHook,
                LsmCrashPoint.AfterSstableTempFileFsyncBeforeRename,
                LsmCrashPoint.AfterSstableRenameBeforeDirectoryFsync,
                LsmCrashPoint.AfterSstableDirectoryFsyncBeforeManifest);

            var metadata = new LsmTableFileMetadata(fileNumber, level: 0, smallest, largest, length, fileName);
            PublishFlushedFile(metadata, filePath);
            AfterSstablePublished?.Invoke(memTable);
            return new LsmFlushResult(metadata, length, filePath);
        }
    }

    /// <summary>Applies the manifest edit for a flushed file under the registry's version gate.</summary>
    private void PublishFlushedFile(LsmTableFileMetadata metadata, string filePath)
    {
        var edit = new LsmVersionEdit();
        edit.AddFile(metadata);
        try
        {
            _manifest.CrashHook = _crashHook;
            if (CompactionRegistry is { } registry)
            {
                registry.ExecuteVersionEdit(() =>
                {
                    _applyEdit(edit);
                    registry.Register(metadata);
                    return true;
                });
            }
            else
            {
                _applyEdit(edit);
            }
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
    }

    /// <summary>Diagnostics: compacts every file of <paramref name="sourceLevel"/> into the next level, now.</summary>
    internal void CompactLevelForTesting(int sourceLevel)
    {
        LsmFileRegistry registry = CompactionRegistry ?? throw new InvalidOperationException("The table has no file registry.");
        lock (_ioGate)
        {
            int maxLevel = ValidatedMaxCompactionLevel();
            var compactor = new LsmCompactor(_tableDirectory, _manifest, registry) { Format = Format, BloomSuffixLength = BloomSuffixLength, ReadMode = ReadMode, BlockCache = BlockCache, Columnar = ColumnarSpec };
            compactor.CompactLevel(sourceLevel, sourceLevel + 1, dropTombstonesAtBottomLevel: sourceLevel + 1 == maxLevel);
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

    /// <summary>
    /// Rewrites every live row-keyed SSTable that has no columnar segment for <see cref="ColumnarSpec"/>, so
    /// each file carries one. A file is copied entry for entry to a new file of the same level and swapped in
    /// with one manifest edit; nothing is merged and no row changes.
    /// </summary>
    /// <returns>The number of files rewritten; 0 when there is no spec or every file already has its segment.</returns>
    internal int BackfillColumnarSegments()
    {
        LsmFileRegistry? registry = CompactionRegistry;
        DataVo.Core.Indexing.Columnar.ColumnarSpec? spec = ColumnarSpec;
        if (registry is null || spec is null || Format != LsmSstableFormat.RowKeyed)
        {
            return 0;
        }

        // The I/O gate keeps flushes and compactions out: the file set only changes here until it is released,
        // and any file they write afterwards already carries the spec's segment.
        lock (_ioGate)
        {
            int rewritten = 0;
            for (int level = 0; level <= MaxCompactionLevel; level++)
            {
                // Level 0 is searched from the highest file number down, so once one of its files gets a new
                // number every newer one must get a newer number too, in the same order.
                bool rewriteRest = false;
                foreach (LsmTableFileMetadata file in _manifest.GetLiveFiles(level).OrderBy(static f => f.FileNumber).ToArray())
                {
                    LsmSstableHandle[] handles = registry.AddRefs([file]);
                    try
                    {
                        if (!handles[0].IsV2())
                        {
                            continue;
                        }

                        SsTableV2Reader reader = handles[0].GetV2Reader(ReadMode, BlockCache);
                        if (!rewriteRest && reader.Columnar is { } segment && segment.Spec.Matches(spec))
                        {
                            continue;
                        }

                        rewriteRest = level == 0;
                        RewriteWithColumnarSegment(registry, file, reader, spec);
                        rewritten++;
                    }
                    finally
                    {
                        registry.Release(handles);
                    }
                }
            }

            return rewritten;
        }
    }

    // Copies one SSTable into a new file that carries the spec's columnar segment and replaces it in the manifest.
    private void RewriteWithColumnarSegment(LsmFileRegistry registry, LsmTableFileMetadata file, SsTableV2Reader reader, DataVo.Core.Indexing.Columnar.ColumnarSpec spec)
    {
        long fileNumber = _manifest.AllocateFileNumber();
        string fileName = $"{fileNumber:D6}.sst";
        string filePath = Path.Combine(_tableDirectory, fileName);
        long length = 0;
        LsmDurableFileOperations.WriteFileAtomically(
            filePath,
            overwrite: false,
            stream =>
            {
                SsTableV2Properties properties = reader.Properties;
                using var writer = new SsTableV2Writer(stream, alignment: 0, (int)Math.Min(properties.EntryCount, int.MaxValue), columnar: spec);
                SsTableV2Cursor cursor = reader.OpenCursor();
                try
                {
                    while (cursor.MoveNext())
                    {
                        writer.Add(cursor.Key, cursor.Sequence, cursor.Type, cursor.Type == LsmValueType.Put ? cursor.Value : default);
                    }
                }
                finally
                {
                    cursor.Release();
                }

                if (properties.NextRowId > 0)
                {
                    writer.SetTableMetadata(properties.NextRowId, properties.LiveRowCount, properties.NextRowIdSequence);
                }

                length = writer.Finish();
            },
            crashHook: null,
            LsmCrashPoint.AfterSstableTempFileFsyncBeforeRename,
            LsmCrashPoint.AfterSstableRenameBeforeDirectoryFsync,
            LsmCrashPoint.AfterSstableDirectoryFsyncBeforeManifest);

        var replacement = new LsmTableFileMetadata(fileNumber, file.Level, file.SmallestInternalKey, file.LargestInternalKey, length, fileName);
        var edit = new LsmVersionEdit();
        edit.AddFile(replacement);
        edit.DeleteFile(file.Level, file.FileNumber);
        try
        {
            registry.ExecuteVersionEdit(() =>
            {
                _applyEdit(edit);
                registry.Register(replacement);
                registry.MarkDeleted([file]);
                return true;
            });
        }
        catch
        {
            DeleteFileIfExists(filePath);
            throw;
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
            if (_stopping || _disposed || CompactionRegistry is null)
            {
                return;
            }

            _pendingCompactionChecks++;
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
            _pendingCompactionChecks--;
            if (_stopping || _disposed || _backgroundError is not null)
            {
                Monitor.PulseAll(_gate);
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
        var compactor = new LsmCompactor(_tableDirectory, _manifest, registry) { Format = Format, BloomSuffixLength = BloomSuffixLength, ReadMode = ReadMode, BlockCache = BlockCache, Columnar = ColumnarSpec };

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
        var compactor = new LsmCompactor(_tableDirectory, _manifest, registry) { Format = Format, BloomSuffixLength = BloomSuffixLength, ReadMode = ReadMode, BlockCache = BlockCache, Columnar = ColumnarSpec };

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

    private void WaitForFlushPipelineNoLock(bool waitForScheduledCompactions = true)
    {
        while ((_frozenQueue.Count > 0 || _workerBusy || (waitForScheduledCompactions && _pendingCompactionChecks > 0))
            && _backgroundError is null)
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

        // The live segment sits at the base path; it is replayed before any rotated (numbered) segment.
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

    private static void DeleteFileIfExists(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }
}
