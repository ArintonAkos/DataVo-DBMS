using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Threading;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Compat;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.StorageEngine.Serialization;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>
/// Minimal LSM-backed storage engine that maps logical 1-based RowIds to encoded LSM user keys.
/// </summary>
public sealed class LsmStorageEngine : IStorageEngine, IFixedWidthPatchStorageEngine, IDisposable
{
    private const int UserKeySize = sizeof(long);
    private const int MaxLevelToScan = LsmTable.DefaultMaxCompactionLevel;
    private static readonly byte[] NextRowIdMetadataKey =
        [0x5F, 0x5F, 0x64, 0x61, 0x74, 0x61, 0x76, 0x6F, 0x5F, 0x6E, 0x65, 0x78, 0x74, 0x5F, 0x72, 0x6F, 0x77, 0x5F, 0x69, 0x64];

    internal const string CommitLogFileName = "commit.wal";

    private readonly string _storageDirectory;
    private readonly LsmWalDurabilityMode _walDurabilityMode;
    private readonly ConcurrentDictionary<(string DatabaseName, string TableName), TableState> _tables = new();
    private readonly LsmCommitLog _commitLog;

    // Serializes "append frame + apply to MemTables" so every table's MemTables always hold a
    // contiguous LSN prefix; that is what makes a table's highest SSTable sequence its flushed point.
    private readonly object _commitGate = new();
    private readonly object _pruneGate = new();
    private readonly AsyncLocal<PendingWrite?> _ambientWrite = new();
    private long _appliedLsn;   // every frame up to here is applied to MemTables
    private readonly object _publishGate = new();
    private long _nextPublishLsn; // next LSN allowed to publish its rows and deferred effects
    private long _prunedThrough;
    private bool _recovering;
    private bool _disposed;

    /// <summary>Creates an LSM storage engine rooted at <paramref name="storageDirectory"/>.</summary>
    public LsmStorageEngine(
        string storageDirectory,
        LsmWalDurabilityMode walDurabilityMode = LsmWalDurabilityMode.StrictFsync)
    {
        PortableStoragePolicy.ThrowIfUnsupported(StorageMode.Lsm);
        ThrowHelper.ThrowIfNullOrWhiteSpace(storageDirectory);
        _storageDirectory = storageDirectory;
        _walDurabilityMode = walDurabilityMode;
        Directory.CreateDirectory(_storageDirectory);
        _commitLog = new LsmCommitLog(Path.Combine(_storageDirectory, CommitLogFileName), walDurabilityMode);

        _recovering = true;
        try
        {
            OpenExistingTables();
            ReplayCommitLog();
        }
        finally
        {
            _recovering = false;
        }

        _appliedLsn = _commitLog.LastLsn;
        _nextPublishLsn = _commitLog.LastLsn + 1;
    }

    /// <summary>
    /// Groups every mutation made by the current flow (one SQL statement or one transaction commit) into
    /// a single commit-log frame, written and made durable when the returned scope is disposed. Scopes
    /// nest; only the outermost one commits. Mutations made outside a scope commit individually.
    /// </summary>
    internal IWriteBatchScope BeginWriteBatch()
    {
        PendingWrite? current = _ambientWrite.Value;
        if (current is not null)
        {
            current.Depth++;
            return new WriteBatchScope(this, current);
        }

        var pending = new PendingWrite();
        _ambientWrite.Value = pending;
        DeferredPublication.SetCurrent(pending.Publication);
        return new WriteBatchScope(this, pending);
    }

    /// <summary>
    /// Inserts a row and takes ownership of <paramref name="rowBytes"/>: the buffer is retained as the
    /// latest in-memory row version (the MemTable copies it into its arena) and must not be mutated by
    /// the caller afterwards.
    /// </summary>
    public long InsertRow(string databaseName, string tableName, byte[] rowBytes)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(rowBytes);
        TableState state = GetOrCreateTable(databaseName, tableName);
        PendingWrite pending = AcquireWrite(out bool ownsWrite);

        long rowId;
        lock (state.SyncRoot)
        {
            rowId = state.NextRowId++;
            pending.Stage(state, rowId, rowBytes);
            pending.Batch.Put(databaseName, tableName, rowId, rowBytes);
            pending.Touch(state, databaseName, tableName);
        }

        CompleteWrite(pending, ownsWrite);
        return rowId;
    }

    /// <summary>
    /// Inserts rows and takes ownership of every buffer in <paramref name="rowsBytes"/> (see
    /// <see cref="InsertRow"/>). Row keys are encoded on the stack per entry — no per-row key or
    /// value copies.
    /// </summary>
    public List<long> InsertRows(string databaseName, string tableName, List<byte[]> rowsBytes)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(rowsBytes);
        TableState state = GetOrCreateTable(databaseName, tableName);
        PendingWrite pending = AcquireWrite(out bool ownsWrite);

        var rowIds = new List<long>(rowsBytes.Count);
        lock (state.SyncRoot)
        {
            foreach (byte[] rowBytes in rowsBytes)
            {
                DataVo.Core.Compat.ThrowHelper.ThrowIfNull(rowBytes);
                long rowId = state.NextRowId++;
                pending.Stage(state, rowId, rowBytes);
                pending.Batch.Put(databaseName, tableName, rowId, rowBytes);
                rowIds.Add(rowId);
            }

            pending.Touch(state, databaseName, tableName);
        }

        CompleteWrite(pending, ownsWrite);
        return rowIds;
    }

    /// <summary>
    /// Returns whether the table currently holds any live (non-tombstoned) rows, without rescanning
    /// SSTables or touching the flush cadence: <see cref="TableState.LatestRows"/> is authoritative
    /// (rebuilt on open, maintained on every mutation). MemTable flushes are size-triggered by
    /// <see cref="LsmTable"/> itself.
    /// </summary>
    public bool HasAnyRows(string databaseName, string tableName)
    {
        TableState state = GetOrCreateTable(databaseName, tableName);
        if (_ambientWrite.Value?.StagedFor(state) is { Count: > 0 })
        {
            return ReadAllRows(databaseName, tableName).Any();
        }

        lock (state.SyncRoot)
        {
            return state.LatestRows.HasAnyLive();
        }
    }

    /// <summary>
    /// Lock-free point read. <see cref="TableState.LatestRows"/> is a concurrent map whose published
    /// value arrays are immutable (patches are copy-on-write), so readers never wait on writers —
    /// a writer parked in its WAL/MemTable critical section cannot stall the read tail.
    /// </summary>
    public byte[] ReadRow(string databaseName, string tableName, long rowId)
    {
        TableState state = GetOrCreateTable(databaseName, tableName);

        // Other sessions see only published rows; the writing flow also sees its own pending writes.
        return Lookup(_ambientWrite.Value, state, rowId, out byte[] rowBytes) switch
        {
            LatestRowLookup.Live => rowBytes.ToArray(),
            LatestRowLookup.Tombstone => throw new RowDeletedException(rowId, tableName),
            _ when rowId > 0 && rowId < state.ReadNextRowId() => throw new RowDeletedException(rowId, tableName),
            _ => throw new RowNotFoundException(rowId, tableName),
        };
    }

    /// <inheritdoc />
    public IEnumerable<(long RowId, byte[] RawRow)> ReadAllRows(string databaseName, string tableName)
    {
        TableState state = GetOrCreateTable(databaseName, tableName);

        List<(long RowId, byte[] RawRow)> rows;
        lock (state.SyncRoot)
        {
            rows = CollectLiveRowsSorted(state);
        }

        IReadOnlyDictionary<long, byte[]?>? staged = _ambientWrite.Value?.StagedFor(state);
        if (staged is not { Count: > 0 })
        {
            return rows;
        }

        var merged = new SortedDictionary<long, byte[]>();
        foreach ((long rowId, byte[] rawRow) in rows)
        {
            merged[rowId] = rawRow;
        }

        foreach ((long rowId, byte[]? value) in staged)
        {
            if (value is null)
            {
                merged.Remove(rowId);
            }
            else
            {
                merged[rowId] = value.ToArray();
            }
        }

        return merged.Select(pair => (pair.Key, pair.Value)).ToList();
    }

    private static LatestRowLookup Lookup(PendingWrite? pending, TableState state, long rowId, out byte[] value)
    {
        if (pending is not null && pending.TryGetStaged(state, rowId, out byte[]? staged))
        {
            value = staged ?? [];
            return staged is null ? LatestRowLookup.Tombstone : LatestRowLookup.Live;
        }

        return state.LatestRows.TryGet(rowId, out value);
    }

    /// <inheritdoc />
    public void DeleteRow(string databaseName, string tableName, long rowId)
    {
        TableState state = GetOrCreateTable(databaseName, tableName);
        PendingWrite pending = AcquireWrite(out bool ownsWrite);

        lock (state.SyncRoot)
        {
            if (Lookup(pending, state, rowId, out _) == LatestRowLookup.Live)
            {
                pending.Stage(state, rowId, null);
                pending.Batch.Delete(databaseName, tableName, rowId);
                pending.Touch(state, databaseName, tableName);
            }
        }

        CompleteWrite(pending, ownsWrite);
    }

    bool IFixedWidthPatchStorageEngine.TryPatchFixedWidthRow(
        string databaseName,
        string tableName,
        long rowId,
        IReadOnlyList<Column> columns,
        ReadOnlySpan<int> ordinals,
        ReadOnlySpan<DataVoFixedWidthValue> values)
    {
        if (ordinals.Length != values.Length)
        {
            throw new ArgumentException("Patch ordinals and values must have the same length.", nameof(values));
        }

        TableState state = GetOrCreateTable(databaseName, tableName);
        PendingWrite pending = AcquireWrite(out bool ownsWrite);
        bool patched = false;
        lock (state.SyncRoot)
        {
            if (Lookup(pending, state, rowId, out byte[] latest) == LatestRowLookup.Live)
            {
                // Copy-on-write: published row buffers are immutable so lock-free readers can never
                // observe a half-applied patch; the new version becomes visible in one reference publish.
                byte[] rowBytes = latest.ToArray();
                patched = true;
                for (int i = 0; i < ordinals.Length; i++)
                {
                    if (!RowSerializer.TryOverwriteFixedWidthCell(rowBytes, columns, ordinals[i], values[i]))
                    {
                        patched = false;
                        break;
                    }
                }

                if (patched)
                {
                    pending.Stage(state, rowId, rowBytes);
                    pending.Batch.Put(databaseName, tableName, rowId, rowBytes);
                    pending.Touch(state, databaseName, tableName);
                }
            }
        }

        CompleteWrite(pending, ownsWrite);
        return patched;
    }

    int IFixedWidthPatchStorageEngine.TryPatchFixedWidthRows(
        string databaseName,
        string tableName,
        IReadOnlyList<Column> columns,
        ReadOnlySpan<int> ordinals,
        IReadOnlyList<FixedWidthPatchOperation> operations)
    {
        if (operations.Count == 0)
        {
            return 0;
        }

        if (ordinals.Length > 2)
        {
            throw new NotSupportedException("LSM fixed-width batch patch currently supports up to two assignments.");
        }

        TableState state = GetOrCreateTable(databaseName, tableName);
        PendingWrite pending = AcquireWrite(out bool ownsWrite);
        int patched = 0;
        lock (state.SyncRoot)
        {
            foreach (FixedWidthPatchOperation operation in operations)
            {
                if (Lookup(pending, state, operation.RowId, out byte[] latest) != LatestRowLookup.Live)
                {
                    continue;
                }

                // Copy-on-write, same as the single-row patch: keep published buffers immutable.
                byte[] rowBytes = latest.ToArray();
                for (int i = 0; i < ordinals.Length; i++)
                {
                    if (!RowSerializer.TryOverwriteFixedWidthCell(rowBytes, columns, ordinals[i], operation.GetValue(i)))
                    {
                        throw new InvalidOperationException("LSM fixed-width batch patch could not overwrite a target cell.");
                    }
                }

                pending.Stage(state, operation.RowId, rowBytes);
                pending.Batch.Put(databaseName, tableName, operation.RowId, rowBytes);
                patched++;
            }

            if (patched > 0)
            {
                pending.Touch(state, databaseName, tableName);
            }
        }

        CompleteWrite(pending, ownsWrite);
        return patched;
    }

    /// <inheritdoc />
    public void DropTable(string databaseName, string tableName)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfDisposed(_disposed, this);
        var marker = new LsmCommitBatch();
        marker.DropTable(databaseName, tableName);
        CommitMarker(marker);
        DropTableCore(databaseName, tableName);
    }

    private void DropTableCore(string databaseName, string tableName)
    {
        (string DatabaseName, string TableName) key = (databaseName, tableName);
        if (_tables.TryRemove(key, out TableState? state))
        {
            lock (state.SyncRoot)
            {
                state.Dispose();
            }
        }

        string tableDirectory = GetTableDirectory(databaseName, tableName);
        if (Directory.Exists(tableDirectory))
        {
            Directory.Delete(tableDirectory, recursive: true);
        }
    }

    /// <inheritdoc />
    public void DropDatabase(string databaseName)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfDisposed(_disposed, this);
        var marker = new LsmCommitBatch();
        marker.DropDatabase(databaseName);
        CommitMarker(marker);
        DropDatabaseCore(databaseName);
    }

    private void DropDatabaseCore(string databaseName)
    {
        foreach ((string DatabaseName, string TableName) key in _tables.Keys)
        {
            if (string.Equals(key.DatabaseName, databaseName, StringComparison.Ordinal)
                && _tables.TryRemove(key, out TableState? state))
            {
                lock (state.SyncRoot)
                {
                    state.Dispose();
                }
            }
        }

        string databaseDirectory = GetDatabaseDirectory(databaseName);
        if (Directory.Exists(databaseDirectory))
        {
            Directory.Delete(databaseDirectory, recursive: true);
        }
    }

    /// <inheritdoc />
    public List<(long NewRowId, byte[] RawRow)> CompactTable(string databaseName, string tableName)
    {
        TableState state = GetOrCreateTable(databaseName, tableName);

        lock (state.SyncRoot)
        {
            state.Table.FlushActiveMemTable();
            state.Table.CompactAllLevelsForMaintenance();

            return CollectLiveRowsSorted(state);
        }
    }

    /// <summary>Disposes cached table writers. Persisted SSTables and manifests remain on disk.</summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        foreach (TableState state in _tables.Values)
        {
            lock (state.SyncRoot)
            {
                state.Dispose(); // flushes the active MemTable to an SSTable
            }
        }

        _tables.Clear();

        // Every applied frame is now in SSTables, so a clean shutdown leaves an empty commit log.
        _commitLog.PruneThrough(Volatile.Read(ref _appliedLsn));
        _commitLog.Dispose();
    }

    private TableState GetOrCreateTable(string databaseName, string tableName)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfDisposed(_disposed, this);
        DataVo.Core.Compat.ThrowHelper.ThrowIfNullOrWhiteSpace(databaseName);
        DataVo.Core.Compat.ThrowHelper.ThrowIfNullOrWhiteSpace(tableName);

        return _tables.GetOrAdd(
            (databaseName, tableName),
            static (key, owner) => owner.CreateTableState(key.DatabaseName, key.TableName),
            this);
    }

    /// <summary>Test hook: runs after a batch is durable and before its effects are published.</summary>
    internal Action<long>? BeforePublishForTesting { get; set; }

    /// <summary>Test hook: overrides the MemTable rotation threshold for tables created afterwards.</summary>
    internal long? MemTableFlushThresholdOverrideBytes { get; set; }

    private TableState CreateTableState(string databaseName, string tableName)
    {
        string tableDirectory = GetTableDirectory(databaseName, tableName);
        Directory.CreateDirectory(tableDirectory);

        var manifest = new LsmManifest(Path.Combine(tableDirectory, "MANIFEST"));
        MigrateLegacyTableWal(tableDirectory, manifest);

        // No per-table WAL: durability comes from the shared commit log, and this table's MemTables
        // hold only mutations whose frames are already in it.
        var table = new LsmTable(tableDirectory, manifest);
        if (MemTableFlushThresholdOverrideBytes is long thresholdOverride)
        {
            table.FlushThresholdBytes = thresholdOverride;
        }

        var registry = new LsmFileRegistry(tableDirectory, manifest);
        table.CompactionRegistry = registry;
        table.BeforeFlushDurabilityBarrier = maxSequence => _commitLog.EnsureDurableThrough(checked((long)maxSequence));
        table.AfterFlush = PruneCommitLog;
        var state = new TableState(tableDirectory, manifest, table, registry);

        Dictionary<long, LatestRowVersion> latestRows = RebuildLatestRowsFromSstables(
            state,
            out long persistedNextRowId,
            out ulong maxSequence);
        if (persistedNextRowId > state.NextRowId)
        {
            state.NextRowId = persistedNextRowId;
        }

        foreach ((long rowId, LatestRowVersion version) in latestRows)
        {
            if (rowId >= state.NextRowId)
            {
                state.NextRowId = rowId + 1;
            }

            if (version.IsTombstone)
            {
                state.LatestRows.SetTombstone(rowId);
            }
            else
            {
                state.LatestRows.Set(rowId, version.Value);
            }
        }

        // Everything in SSTables is durable; only commit-log frames above this point still need replay.
        state.FlushedLsn = checked((long)maxSequence);
        return state;
    }

    /// <summary>
    /// Directories written before the shared commit log kept a WAL per table. Replay those segments once
    /// through a WAL-backed table, flush them to SSTables, and remove them, so nothing is lost on upgrade.
    /// </summary>
    private void MigrateLegacyTableWal(string tableDirectory, LsmManifest manifest)
    {
        if (!Directory.EnumerateFiles(tableDirectory, "active*.wal").Any())
        {
            return;
        }

        using var legacy = new LsmTable(tableDirectory, manifest, Path.Combine(tableDirectory, "active.wal"), _walDurabilityMode);
        legacy.FlushActiveMemTable();
    }

    /// <summary>Opens every table directory on disk, so the LSN floor covers all persisted sequence numbers.</summary>
    private void OpenExistingTables()
    {
        long floor = 0;
        foreach (string databaseDirectory in Directory.EnumerateDirectories(_storageDirectory))
        {
            foreach (string tableDirectory in Directory.EnumerateDirectories(databaseDirectory))
            {
                if (!File.Exists(Path.Combine(tableDirectory, "MANIFEST")))
                {
                    continue;
                }

                TableState state = GetOrCreateTable(Path.GetFileName(databaseDirectory), Path.GetFileName(tableDirectory));
                floor = Math.Max(floor, state.FlushedLsn);
            }
        }

        _commitLog.EnsureLsnAtLeast(floor);
    }

    /// <summary>
    /// Re-applies every commit-log frame a table has not flushed yet. An operation applies only when its
    /// LSN is above the flushed point of the table directory now on disk: that skips work already in
    /// SSTables and keeps an old drop marker from deleting a table that was re-created after it.
    /// </summary>
    private void ReplayCommitLog()
    {
        Span<byte> userKey = stackalloc byte[UserKeySize];
        foreach (LsmCommitFrame frame in _commitLog.ReadAll())
        {
            ulong sequence = checked((ulong)frame.Lsn);
            var touched = new HashSet<TableState>();
            foreach (LsmCommitOperation operation in frame.Operations)
            {
                switch (operation.Kind)
                {
                    case LsmCommitOperationKind.DropTable:
                        if (frame.Lsn > GetOrCreateTable(operation.Database, operation.Table).FlushedLsn)
                        {
                            DropTableCore(operation.Database, operation.Table);
                        }

                        continue;

                    case LsmCommitOperationKind.DropDatabase:
                        ReplayDropDatabase(operation.Database, frame.Lsn);
                        continue;
                }

                TableState state = GetOrCreateTable(operation.Database, operation.Table);
                if (frame.Lsn <= state.FlushedLsn)
                {
                    continue;
                }

                EncodeRowId(operation.RowId, userKey);
                if (operation.Kind == LsmCommitOperationKind.Put)
                {
                    state.Table.Put(userKey, sequence, operation.Value);
                    state.LatestRows.Set(operation.RowId, operation.Value);
                }
                else
                {
                    state.Table.Delete(userKey, sequence);
                    state.LatestRows.SetTombstone(operation.RowId);
                }

                if (operation.RowId >= state.NextRowId)
                {
                    state.NextRowId = operation.RowId + 1;
                }

                touched.Add(state);
            }

            foreach (TableState state in touched)
            {
                state.Table.Put(NextRowIdMetadataKey, sequence, EncodeNextRowIdMetadata(state.NextRowId));
            }
        }
    }

    private void ReplayDropDatabase(string databaseName, long lsn)
    {
        string databaseDirectory = GetDatabaseDirectory(databaseName);
        if (!Directory.Exists(databaseDirectory))
        {
            return;
        }

        foreach (string tableDirectory in Directory.EnumerateDirectories(databaseDirectory))
        {
            string tableName = Path.GetFileName(tableDirectory);
            if (lsn > GetOrCreateTable(databaseName, tableName).FlushedLsn)
            {
                DropTableCore(databaseName, tableName);
            }
        }

        if (!Directory.EnumerateFileSystemEntries(databaseDirectory).Any())
        {
            Directory.Delete(databaseDirectory);
        }
    }

    // A write outside any statement scope commits on its own; reuse one buffer per thread for it so
    // single-call writes (typed inserts, compiled updates) do not allocate batch objects every time.
    [ThreadStatic]
    private static PendingWrite? t_reusableWrite;

    private PendingWrite AcquireWrite(out bool ownsWrite)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfDisposed(_disposed, this);
        PendingWrite? ambient = _ambientWrite.Value;
        ownsWrite = ambient is null;
        if (ambient is not null)
        {
            return ambient;
        }

        PendingWrite pending = t_reusableWrite ?? new PendingWrite();
        t_reusableWrite = null;
        return pending;
    }

    private void CompleteWrite(PendingWrite pending, bool ownsWrite)
    {
        if (!ownsWrite)
        {
            return;
        }

        try
        {
            Commit(pending);
        }
        finally
        {
            pending.Reset();
            t_reusableWrite = pending;
        }
    }

    /// <summary>
    /// Writes a pending batch as one commit-log frame and applies it to the MemTables, then waits for
    /// durability outside the gate so concurrent committers share one fsync.
    /// </summary>
    private void Commit(PendingWrite pending)
    {
        try
        {
            if (pending.Batch.IsEmpty)
            {
                pending.Publication.RunPublish();
                return;
            }

            pending.Batch.CollapseToLatestPerRow();
            LsmCommitTicket ticket;
            lock (_commitGate)
            {
                ticket = _commitLog.Append(pending.Batch);
                ApplyToMemTables(pending, checked((ulong)ticket.Lsn));
                Volatile.Write(ref _appliedLsn, ticket.Lsn);
            }

            bool durable = false;
            try
            {
                ticket.Wait();
                durable = true;
            }
            finally
            {
                // Rows, indexes, and notifications become visible only now, and strictly in commit order.
                PublishInOrder(ticket.Lsn, durable ? () => PublishStaged(pending) : null);
            }
        }
        finally
        {
            pending.Publication.RunFinally();
            pending.Reset();
        }
    }

    /// <summary>Drops a failed statement's pending work: nothing was written, so nothing is published.</summary>
    private void Discard(PendingWrite pending)
    {
        try
        {
            pending.Publication.DiscardPublish();
        }
        finally
        {
            pending.Publication.RunFinally();
            pending.Reset();
        }
    }

    private void PublishStaged(PendingWrite pending)
    {
        foreach ((TableState state, Dictionary<long, byte[]?> rows) in pending.StagedTables)
        {
            lock (state.SyncRoot)
            {
                foreach ((long rowId, byte[]? value) in rows)
                {
                    if (value is null)
                    {
                        state.LatestRows.SetTombstone(rowId);
                    }
                    else
                    {
                        state.LatestRows.Set(rowId, value);
                    }
                }
            }
        }

        pending.Publication.RunPublish();
    }

    /// <summary>
    /// Waits until every lower LSN is published, runs <paramref name="publish"/> on this flow (locks and
    /// deferred actions are thread-affine), then lets the next LSN publish. Always advances, even when
    /// publishing is skipped or fails, so one failed commit cannot stall every later one.
    /// </summary>
    private void PublishInOrder(long lsn, Action? publish)
    {
        lock (_publishGate)
        {
            while (_nextPublishLsn < lsn)
            {
                Monitor.Wait(_publishGate);
            }
        }

        try
        {
            if (publish is not null)
            {
                BeforePublishForTesting?.Invoke(lsn);
                publish();
            }
        }
        finally
        {
            lock (_publishGate)
            {
                _nextPublishLsn = lsn + 1;
                Monitor.PulseAll(_publishGate);
            }
        }
    }

    private void ApplyToMemTables(PendingWrite pending, ulong sequence)
    {
        Span<byte> userKey = stackalloc byte[UserKeySize];
        foreach (LsmCommitOperation operation in pending.Batch.Operations)
        {
            if (!_tables.TryGetValue((operation.Database, operation.Table), out TableState? state))
            {
                continue; // dropped while the statement was running
            }

            EncodeRowId(operation.RowId, userKey);
            if (operation.Kind == LsmCommitOperationKind.Put)
            {
                state.Table.Put(userKey, sequence, operation.Value);
            }
            else
            {
                state.Table.Delete(userKey, sequence);
            }
        }

        foreach ((TableState state, _, _) in pending.TouchedTables)
        {
            state.Table.Put(NextRowIdMetadataKey, sequence, EncodeNextRowIdMetadata(state.ReadNextRowId()));
        }
    }

    // Drop markers are durable before any file is deleted, so a crash mid-drop replays the drop.
    private void CommitMarker(LsmCommitBatch marker)
    {
        LsmCommitTicket ticket;
        lock (_commitGate)
        {
            ticket = _commitLog.Append(marker);
            Volatile.Write(ref _appliedLsn, ticket.Lsn);
        }

        try
        {
            _commitLog.EnsureDurableThrough(ticket.Lsn);
        }
        finally
        {
            PublishInOrder(ticket.Lsn, publish: null);
        }
    }

    /// <summary>
    /// Drops commit-log frames whose effects are in SSTables. Reads the applied watermark first: any frame
    /// at or below it is in some MemTable, so a table still holding it reports it as unflushed.
    /// Never blocks on the commit gate, because it runs on flush threads that writers may be waiting for.
    /// </summary>
    private void PruneCommitLog()
    {
        if (_recovering || !Monitor.TryEnter(_pruneGate))
        {
            return;
        }

        try
        {
            long watermark = Volatile.Read(ref _appliedLsn);
            foreach (TableState state in _tables.Values)
            {
                if (state.Table.OldestUnflushedSequence is ulong oldest)
                {
                    watermark = Math.Min(watermark, checked((long)oldest) - 1);
                }
            }

            if (watermark > _prunedThrough)
            {
                _commitLog.PruneThrough(watermark);
                _prunedThrough = watermark;
            }
        }
        finally
        {
            Monitor.Exit(_pruneGate);
        }
    }

    private string GetDatabaseDirectory(string databaseName) =>
        Path.Combine(_storageDirectory, databaseName);

    private string GetTableDirectory(string databaseName, string tableName) =>
        Path.Combine(GetDatabaseDirectory(databaseName), tableName);

    private static List<(long RowId, byte[] RawRow)> CollectLiveRowsSorted(TableState state)
    {
        // LatestRows is authoritative (seeded from SSTables on open, maintained on every mutation),
        // so scans read it directly instead of rescanning SSTables — no flush needed. Slots are laid
        // out by row id, so the collected rows come out already sorted. Caller holds SyncRoot.
        var rows = new List<(long RowId, byte[] RawRow)>();
        state.LatestRows.ForEachLive(rows, static (rows, rowId, value) => rows.Add((rowId, value.ToArray())));
        return rows;
    }

    private static Dictionary<long, LatestRowVersion> RebuildLatestRowsFromSstables(
        TableState state,
        out long persistedNextRowId,
        out ulong maxSequence)
    {
        var latest = new Dictionary<long, LatestRowVersion>();
        persistedNextRowId = 1;
        maxSequence = 0;
        ulong persistedNextRowIdSeqno = 0;
        foreach (SstableEntry entry in ReadAllSstableEntries(state))
        {
            ulong seqno = InternalKey.Sequence(entry.InternalKey);
            maxSequence = Math.Max(maxSequence, seqno);
            ReadOnlySpan<byte> userKey = InternalKey.UserKey(entry.InternalKey);
            if (IsNextRowIdMetadataKey(userKey))
            {
                if (seqno >= persistedNextRowIdSeqno)
                {
                    persistedNextRowId = DecodeNextRowIdMetadata(entry.Value);
                    persistedNextRowIdSeqno = seqno;
                }

                continue;
            }

            long rowId = DecodeRowId(userKey);
            if (latest.TryGetValue(rowId, out LatestRowVersion existing) && existing.Sequence >= seqno)
            {
                continue;
            }

            LsmValueType valueType = InternalKey.ValueType(entry.InternalKey);
            latest[rowId] = new LatestRowVersion(
                entry.Value.ToArray(),
                valueType == LsmValueType.Deletion,
                seqno);
        }

        return latest;
    }

    private static IEnumerable<SstableEntry> ReadAllSstableEntries(TableState state)
    {
        foreach (LsmTableFileMetadata file in GetLiveFilesNewestFirst(state.Manifest))
        {
            byte[] bytes = File.ReadAllBytes(Path.Combine(state.TableDirectory, file.FileName));
            SsTableReader.Load(bytes);
            foreach (SstableEntry entry in ReadSstableEntries(bytes))
            {
                yield return entry;
            }
        }
    }

    private static IEnumerable<LsmTableFileMetadata> GetLiveFilesNewestFirst(LsmManifest manifest)
    {
        var files = new List<LsmTableFileMetadata>();
        for (int level = 0; level <= MaxLevelToScan; level++)
        {
            files.AddRange(manifest.GetLiveFiles(level));
        }

        foreach (LsmTableFileMetadata file in files.OrderByDescending(file => file.FileNumber))
        {
            yield return file;
        }
    }

    private static List<SstableEntry> ReadSstableEntries(byte[] bytes)
    {
        if (!SsTableFormat.TryReadFooter(bytes, out SsTableBlockHandle indexBlock, out _))
        {
            throw new InvalidDataException("SSTable footer is missing, corrupt, or points outside the table.");
        }

        SsTableBlockHandle dataBlock = ReadDataBlockHandle(SliceBlock(bytes, indexBlock));
        ReadOnlySpan<byte> data = SliceBlock(bytes, dataBlock);
        var entries = new List<SstableEntry>();
        int offset = 0;
        while (offset < data.Length)
        {
            int keyLength = ReadInt32(data, ref offset);
            int valueLength = ReadInt32(data, ref offset);
            if (keyLength < InternalKey.TagSize || valueLength < 0)
            {
                throw new InvalidDataException("SSTable data block contains a malformed entry length.");
            }

            byte[] internalKey = ReadSpan(data, ref offset, keyLength).ToArray();
            byte[] value = ReadSpan(data, ref offset, valueLength).ToArray();
            entries.Add(new SstableEntry(internalKey, value));
        }

        return entries;
    }

    private static SsTableBlockHandle ReadDataBlockHandle(ReadOnlySpan<byte> indexBlock)
    {
        int offset = 0;
        int count = ReadInt32(indexBlock, ref offset);
        if (count != 1)
        {
            throw new InvalidDataException($"SSTable v1 scan expects exactly one data block, found {count}.");
        }

        long dataOffset = ReadInt64(indexBlock, ref offset);
        int dataLength = ReadInt32(indexBlock, ref offset);
        return new SsTableBlockHandle(dataOffset, dataLength);
    }

    private static ReadOnlySpan<byte> SliceBlock(byte[] source, SsTableBlockHandle handle)
    {
        if (handle.Offset < 0
            || handle.Length <= 0
            || handle.Offset > int.MaxValue
            || handle.Offset + handle.Length > source.Length)
        {
            throw new InvalidDataException("SSTable block handle points outside the table.");
        }

        return source.AsSpan((int)handle.Offset, handle.Length);
    }

    private static int ReadInt32(ReadOnlySpan<byte> source, ref int offset)
    {
        ReadOnlySpan<byte> span = ReadSpan(source, ref offset, sizeof(int));
        return BinaryPrimitives.ReadInt32LittleEndian(span);
    }

    private static long ReadInt64(ReadOnlySpan<byte> source, ref int offset)
    {
        ReadOnlySpan<byte> span = ReadSpan(source, ref offset, sizeof(long));
        return BinaryPrimitives.ReadInt64LittleEndian(span);
    }

    private static ReadOnlySpan<byte> ReadSpan(ReadOnlySpan<byte> source, ref int offset, int length)
    {
        if (length < 0 || offset < 0 || offset > source.Length - length)
        {
            throw new InvalidDataException("SSTable block is truncated.");
        }

        ReadOnlySpan<byte> span = source.Slice(offset, length);
        offset += length;
        return span;
    }

    private static void EncodeRowId(long rowId, Span<byte> destination)
    {
        if (destination.Length != UserKeySize)
        {
            throw new ArgumentException("LSM storage engine expected an 8-byte RowId destination.", nameof(destination));
        }

        InternalKey.EncodeInt64UserKey(destination, rowId);
    }

    private static long DecodeRowId(ReadOnlySpan<byte> userKey)
    {
        if (userKey.Length != UserKeySize)
        {
            throw new InvalidDataException("LSM storage engine expected an 8-byte RowId user key.");
        }

        ulong flipped = BinaryPrimitives.ReadUInt64BigEndian(userKey);
        return unchecked((long)(flipped ^ (ulong)long.MinValue));
    }

    private static bool IsNextRowIdMetadataKey(ReadOnlySpan<byte> userKey) =>
        userKey.SequenceEqual(NextRowIdMetadataKey);

    private static byte[] EncodeNextRowIdMetadata(long nextRowId)
    {
        var value = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(value, nextRowId);
        return value;
    }

    private static long DecodeNextRowIdMetadata(ReadOnlySpan<byte> value)
    {
        if (value.Length != sizeof(long))
        {
            throw new InvalidDataException("LSM next-row-id metadata value is malformed.");
        }

        long nextRowId = BinaryPrimitives.ReadInt64LittleEndian(value);
        if (nextRowId < 1)
        {
            throw new InvalidDataException("LSM next-row-id metadata value must be positive.");
        }

        return nextRowId;
    }

    private sealed class PendingWrite
    {
        private readonly List<(TableState State, string Database, string Table)> _touched = [];

        private readonly Dictionary<TableState, Dictionary<long, byte[]?>> _staged = []; // TableState uses reference equality

        public LsmCommitBatch Batch { get; } = new();

        /// <summary>In-memory effects (indexes, MVCC, notifications, lock releases) held until publication.</summary>
        public DeferredPublication Publication { get; } = new();

        public IEnumerable<KeyValuePair<TableState, Dictionary<long, byte[]?>>> StagedTables => _staged;

        /// <summary>Records a row's pending version (<see langword="null"/> = deleted) for publication.</summary>
        public void Stage(TableState state, long rowId, byte[]? value)
        {
            if (!_staged.TryGetValue(state, out Dictionary<long, byte[]?>? rows))
            {
                rows = [];
                _staged[state] = rows;
            }

            rows[rowId] = value;
        }

        public bool TryGetStaged(TableState state, long rowId, out byte[]? value)
        {
            value = null;
            return _staged.TryGetValue(state, out Dictionary<long, byte[]?>? rows) && rows.TryGetValue(rowId, out value);
        }

        public IReadOnlyDictionary<long, byte[]?>? StagedFor(TableState state) =>
            _staged.TryGetValue(state, out Dictionary<long, byte[]?>? rows) ? rows : null;

        public int Depth { get; set; } = 1;

        public IReadOnlyList<(TableState State, string Database, string Table)> TouchedTables => _touched;

        public void Touch(TableState state, string database, string table)
        {
            foreach ((TableState existing, _, _) in _touched)
            {
                if (ReferenceEquals(existing, state))
                {
                    return;
                }
            }

            _touched.Add((state, database, table));
        }

        public void Reset()
        {
            Batch.Clear();
            _touched.Clear();
            _staged.Clear();
        }
    }

    private sealed class WriteBatchScope(LsmStorageEngine owner, PendingWrite pending) : IWriteBatchScope
    {
        private bool _completed;
        private bool _disposed;

        public void Complete() => _completed = true;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            if (--pending.Depth > 0)
            {
                return;
            }

            owner._ambientWrite.Value = null;
            DeferredPublication.SetCurrent(null); // deferred actions must now run for real

            if (_completed)
            {
                owner.Commit(pending);
            }
            else
            {
                // A failed statement leaves nothing behind: no commit record, no visible change.
                owner.Discard(pending);
            }
        }
    }

    private sealed class TableState : IDisposable
    {
        private long _nextRowId = 1;

        public TableState(string tableDirectory, LsmManifest manifest, LsmTable table, LsmFileRegistry fileRegistry)
        {
            TableDirectory = tableDirectory;
            Manifest = manifest;
            Table = table;
            FileRegistry = fileRegistry;
        }

        public object SyncRoot { get; } = new();

        public string TableDirectory { get; }

        public LsmManifest Manifest { get; }

        public LsmTable Table { get; }

        public LsmFileRegistry FileRegistry { get; }

        /// <summary>
        /// Authoritative row-id → latest-version map. Read lock-free by point reads; mutated only
        /// under <see cref="SyncRoot"/>. Published row buffers are immutable (patches copy-on-write),
        /// so a published slot is always a complete row image.
        /// </summary>
        public LsmLatestRowStore LatestRows { get; } = new();

        public long NextRowId
        {
            get => Volatile.Read(ref _nextRowId);
            set => Volatile.Write(ref _nextRowId, value);
        }

        public long ReadNextRowId() => Volatile.Read(ref _nextRowId);

        /// <summary>Highest sequence number already durable in this table's SSTables when it was opened.</summary>
        public long FlushedLsn { get; set; }

        public void Dispose()
        {
            Table.FlushActiveMemTable();
            Table.Dispose();
        }
    }

    private readonly record struct LatestRowVersion(byte[] Value, bool IsTombstone, ulong Sequence);

    private readonly record struct SstableEntry(byte[] InternalKey, byte[] Value);
}
