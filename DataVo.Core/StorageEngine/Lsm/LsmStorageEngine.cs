using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Threading;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Compat;
using DataVo.Core.Exceptions;
using DataVo.Core.Indexing.HNSW;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.StorageEngine.Lsm.V2;
using DataVo.Core.StorageEngine.Serialization;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>Turns a row as stored into the row callers see (a clustered table puts its key back).</summary>
internal interface IStoredRowMaterializer
{
    byte[] Materialize(RowKey key, ReadOnlySpan<byte> stored);
}

/// <summary>
/// LSM-backed storage engine. Rows are stored under their 16-byte <see cref="RowKey"/>; tables without a
/// clustered key get engine-assigned integer keys (1, 2, 3, ...).
/// </summary>
public sealed partial class LsmStorageEngine : IFixedWidthPatchStorageEngine, ILsmIndexStore, IDisposable
{
    private const int UserKeySize = LsmRowKeyCodec.RowKeySize;
    private const int MaxLevelToScan = LsmTable.DefaultMaxCompactionLevel;
    private static readonly byte[] NextRowIdMetadataKey = LsmRowKeyCodec.TableMetadataKey;

    /// <summary>Marks a deleted row in <see cref="ResolveRow"/> results; matched by reference.</summary>
    private static readonly byte[] DeletedMarker = new byte[1];

    internal const string CommitLogFileName = "commit.wal";

    private readonly string _storageDirectory;
    private readonly LsmWalDurabilityMode _walDurabilityMode;
    private readonly LsmReadMode _readMode;
    private readonly ConcurrentDictionary<(string DatabaseName, string TableName), TableState> _tables = new();
    private readonly LsmCommitLog _commitLog;

    // Serializes "append frame + apply to MemTables" so every table's MemTables always hold a
    // contiguous LSN prefix; that is what makes a table's highest SSTable sequence its flushed point.
    private readonly object _commitGate = new();
    private readonly object _pruneGate = new();
    private readonly AsyncLocal<PendingWrite?> _ambientWrite = new();
    private long _appliedLsn;   // every frame up to here is applied to MemTables
    private readonly object _publishGate = new();
    private static readonly TimeSpan PublicationTimeout = TimeSpan.FromSeconds(60);
    private long _nextPublishLsn; // next LSN allowed to publish its rows and deferred effects
    private long _prunedThrough;
    private bool _recovering;
    private bool _disposed;
    private bool _crashed; // SimulateCrashForTesting ran: nothing may touch the log any more

    /// <summary>Creates an LSM storage engine rooted at <paramref name="storageDirectory"/>.</summary>
    public LsmStorageEngine(
        string storageDirectory,
        LsmWalDurabilityMode walDurabilityMode = LsmWalDurabilityMode.StrictFsync,
        LsmReadMode readMode = LsmReadMode.Auto)
    {
        PortableStoragePolicy.ThrowIfUnsupported(StorageMode.Lsm);
        ThrowHelper.ThrowIfNullOrWhiteSpace(storageDirectory);
        _storageDirectory = storageDirectory;
        _walDurabilityMode = walDurabilityMode;
        _readMode = readMode;
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
    public RowKey InsertRow(string databaseName, string tableName, byte[] rowBytes)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(rowBytes);
        TableState state = GetOrCreateTable(databaseName, tableName);
        PendingWrite pending = AcquireWrite(out bool ownsWrite);

        RowKey rowId;
        lock (state.SyncRoot)
        {
            rowId = state.NextRowId++;
            pending.Stage(state, rowId, rowBytes);
            pending.Batch.Put(databaseName, tableName, rowId, rowBytes);
            pending.Touch(state, databaseName, tableName);
            pending.AddLiveDelta(state, 1);
        }

        CompleteWrite(pending, ownsWrite);
        return rowId;
    }

    /// <summary>
    /// Inserts rows and takes ownership of every buffer in <paramref name="rowsBytes"/> (see
    /// <see cref="InsertRow"/>). Row keys are encoded on the stack per entry — no per-row key or
    /// value copies.
    /// </summary>
    public List<RowKey> InsertRows(string databaseName, string tableName, List<byte[]> rowsBytes)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(rowsBytes);
        TableState state = GetOrCreateTable(databaseName, tableName);
        PendingWrite pending = AcquireWrite(out bool ownsWrite);

        var rowIds = new List<RowKey>(rowsBytes.Count);
        lock (state.SyncRoot)
        {
            pending.Reserve(state, rowsBytes.Count);
            foreach (byte[] rowBytes in rowsBytes)
            {
                DataVo.Core.Compat.ThrowHelper.ThrowIfNull(rowBytes);
                RowKey rowId = state.NextRowId++;
                pending.Stage(state, rowId, rowBytes);
                pending.Batch.Put(databaseName, tableName, rowId, rowBytes);
                rowIds.Add(rowId);
            }

            pending.Touch(state, databaseName, tableName);
            pending.AddLiveDelta(state, rowsBytes.Count);
        }

        CompleteWrite(pending, ownsWrite);
        return rowIds;
    }

    /// <summary>
    /// Inserts a row under a caller-chosen key (a clustered primary key) and takes ownership of
    /// <paramref name="rowBytes"/> like <see cref="InsertRow"/>. Returns <see langword="false"/>, writing
    /// nothing, when a live row (committed, or staged by this flow) already has the key.
    /// </summary>
    /// <remarks>
    /// An integer key also moves the table's next engine row id past it, so a table that mixes chosen
    /// and engine-assigned keys never hands out a key that is taken; replay does the same.
    /// </remarks>
    internal bool TryInsertRowWithKey(string databaseName, string tableName, RowKey key, byte[] rowBytes)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(rowBytes);
        TableState state = GetOrCreateTable(databaseName, tableName);
        PendingWrite pending = AcquireWrite(out bool ownsWrite);

        bool inserted = false;
        lock (state.SyncRoot)
        {
            if (Lookup(pending, state, key, out _) != LatestRowLookup.Live)
            {
                state.NextRowId = Math.Max(state.NextRowId, NextRowIdAfter(key));
                pending.Stage(state, key, rowBytes);
                pending.Batch.Put(databaseName, tableName, key, rowBytes);
                pending.Touch(state, databaseName, tableName);
                pending.AddLiveDelta(state, 1);
                inserted = true;
            }
        }

        CompleteWrite(pending, ownsWrite);
        return inserted;
    }

    /// <summary>
    /// Inserts rows under caller-chosen keys, all or nothing: when any key is already live (or repeats
    /// within <paramref name="keys"/>) nothing is written and that key is returned in
    /// <paramref name="duplicate"/>. Takes ownership of the row buffers like <see cref="InsertRows"/>.
    /// </summary>
    internal bool TryInsertRowsWithKeys(
        string databaseName,
        string tableName,
        IReadOnlyList<RowKey> keys,
        IReadOnlyList<byte[]> rowsBytes,
        out RowKey duplicate)
    {
        if (keys.Count != rowsBytes.Count)
        {
            throw new ArgumentException("Every row needs exactly one key.", nameof(keys));
        }

        duplicate = default;
        TableState state = GetOrCreateTable(databaseName, tableName);
        PendingWrite pending = AcquireWrite(out bool ownsWrite);
        bool inserted = false;
        try
        {
            lock (state.SyncRoot)
            {
                // Check every key before staging any row, so a rejected batch leaves nothing behind. The file
                // set is pinned once for the batch, not once per key.
                HashSet<RowKey>? seen = keys.Count > 1 ? new HashSet<RowKey>(keys.Count) : null;
                TableVersion? version = null;
                try
                {
                    for (int i = 0; i < keys.Count; i++)
                    {
                        RowKey key = keys[i];
                        if ((seen is not null && !seen.Add(key)) || IsLiveForInsert(pending, state, key, ref version))
                        {
                            duplicate = key;
                            return false;
                        }
                    }
                }
                finally
                {
                    version?.Release();
                }

                pending.Reserve(state, keys.Count);
                long nextRowId = state.NextRowId;
                for (int i = 0; i < keys.Count; i++)
                {
                    DataVo.Core.Compat.ThrowHelper.ThrowIfNull(rowsBytes[i]);
                    nextRowId = Math.Max(nextRowId, NextRowIdAfter(keys[i]));
                    pending.Stage(state, keys[i], rowsBytes[i]);
                    pending.Batch.Put(databaseName, tableName, keys[i], rowsBytes[i]);
                }

                state.NextRowId = nextRowId;
                if (keys.Count > 0)
                {
                    pending.Touch(state, databaseName, tableName);
                    pending.AddLiveDelta(state, keys.Count);
                }

                inserted = true;
            }
        }
        finally
        {
            // A rejected batch staged nothing; completing it commits nothing (or returns the reusable buffer).
            CompleteWrite(pending, ownsWrite);
        }

        return inserted;
    }

    /// <summary>
    /// Returns whether the table currently holds any live rows, from the table's live-row counter
    /// (maintained in commit order and persisted with every commit), without reading rows.
    /// </summary>
    public bool HasAnyRows(string databaseName, string tableName)
    {
        TableState state = GetOrCreateTable(databaseName, tableName);
        if (_ambientWrite.Value?.StagedFor(state) is { Count: > 0 })
        {
            return ReadAllRows(databaseName, tableName).Any();
        }

        return state.LiveRowCount > 0;
    }

    /// <summary>Committed live rows of the table (planner estimate; ignores this flow's staged writes).</summary>
    public long EstimateRowCount(string databaseName, string tableName) =>
        GetOrCreateTable(databaseName, tableName).LiveRowCount;

    /// <summary>
    /// Lock-free point read: this flow's staged writes, then the unflushed overlay, then the SSTables
    /// (memory-mapped; one Bloom probe and two binary searches per file). Readers never wait on writers.
    /// </summary>
    public byte[] ReadRow(string databaseName, string tableName, RowKey rowId) =>
        ReadRow(databaseName, tableName, rowId, materializer: null);

    /// <summary>
    /// <see cref="ReadRow(string, string, RowKey)"/> with the stored bytes turned into the caller's row by
    /// <paramref name="materializer"/> (in place of the copy a read makes anyway).
    /// </summary>
    internal byte[] ReadRow(string databaseName, string tableName, RowKey rowId, IStoredRowMaterializer? materializer)
    {
        TableState state = GetOrCreateTable(databaseName, tableName);

        // Other sessions see only published rows; the writing flow also sees its own pending writes.
        (byte[]? row, bool owned) = ResolveRow(_ambientWrite.Value, state, rowId, materializer);
        if (row is null)
        {
            throw rowId.IsInt64 && rowId.AsInt64() > 0 && rowId.AsInt64() < state.ReadNextRowId()
                ? new RowDeletedException(rowId, tableName)
                : new RowNotFoundException(rowId, tableName);
        }

        if (ReferenceEquals(row, DeletedMarker))
        {
            throw new RowDeletedException(rowId, tableName);
        }

        return owned ? row : Materialize(materializer, rowId, row);
    }

    private static byte[] Materialize(IStoredRowMaterializer? materializer, RowKey key, ReadOnlySpan<byte> stored) =>
        materializer?.Materialize(key, stored) ?? stored.ToArray();

    /// <inheritdoc />
    public IEnumerable<(RowKey RowId, byte[] RawRow)> ReadAllRows(string databaseName, string tableName) =>
        ReadAllRows(databaseName, tableName, materializer: null);

    /// <summary><see cref="ReadAllRows(string, string)"/> with each stored row materialized (see <see cref="ReadRow(string, string, RowKey, IStoredRowMaterializer?)"/>).</summary>
    internal IEnumerable<(RowKey RowId, byte[] RawRow)> ReadAllRows(string databaseName, string tableName, IStoredRowMaterializer? materializer)
    {
        TableState state = GetOrCreateTable(databaseName, tableName);

        List<(RowKey RowId, byte[] RawRow)> rows = CollectLiveRowsSorted(state, materializer);

        IReadOnlyDictionary<RowKey, byte[]?>? staged = _ambientWrite.Value?.StagedFor(state);
        if (staged is not { Count: > 0 })
        {
            return rows;
        }

        var merged = new SortedDictionary<RowKey, byte[]>();
        foreach ((RowKey rowId, byte[] rawRow) in rows)
        {
            merged[rowId] = rawRow;
        }

        foreach ((RowKey rowId, byte[]? value) in staged)
        {
            if (value is null)
            {
                merged.Remove(rowId);
            }
            else
            {
                merged[rowId] = Materialize(materializer, rowId, value);
            }
        }

        return merged.Select(pair => (pair.Key, pair.Value)).ToList();
    }

    /// <summary>Whether a live row has <paramref name="key"/>, as this flow sees it (its own staged writes included).</summary>
    /// <remarks>Copies no row: an SSTable hit reports only whether its newest version is a put.</remarks>
    internal bool IsRowLive(string databaseName, string tableName, RowKey key)
    {
        TableState state = GetOrCreateTable(databaseName, tableName);
        if (_ambientWrite.Value is { } pending && pending.TryGetStaged(state, key, out byte[]? staged))
        {
            return staged is not null;
        }

        switch (state.LatestRows.TryGet(key, out _))
        {
            case LatestRowLookup.Live:
                return true;
            case LatestRowLookup.Tombstone:
                return false;
        }

        TableVersion version = state.AcquireVersion();
        try
        {
            return IsLiveInSstables(version, key);
        }
        finally
        {
            version.Release();
        }
    }

    /// <summary>Whether the newest version of <paramref name="key"/> in the file set is a put; copies no row.</summary>
    private bool IsLiveInSstables(TableVersion version, RowKey key)
    {
        SstableHit best = default;
        int bestLevel = -1;
        foreach (LsmSstableHandle file in version.Files)
        {
            int level = file.Metadata.Level;
            if (bestLevel >= 0 && level != bestLevel)
            {
                break;
            }

            SstableHit hit = file.GetV2Reader(_readMode).Lookup(
                key,
                ulong.MaxValue,
                0,
                static (type, sequence, _, _) => new SstableHit(true, sequence, type == LsmValueType.Put ? LiveMarker : null),
                default(SstableHit));
            if (hit.Found && (!best.Found || hit.Sequence > best.Sequence))
            {
                best = hit;
                bestLevel = level;
            }
        }

        return best.Found && best.Value is not null;
    }

    /// <summary>Whether <paramref name="key"/> is live for this flow, pinning the file set on first need.</summary>
    private bool IsLiveForInsert(PendingWrite pending, TableState state, RowKey key, ref TableVersion? version)
    {
        if (pending.TryGetStaged(state, key, out byte[]? staged))
        {
            return staged is not null;
        }

        switch (state.LatestRows.TryGet(key, out _))
        {
            case LatestRowLookup.Live:
                return true;
            case LatestRowLookup.Tombstone:
                return false;
        }

        version ??= state.AcquireVersion();
        return IsLiveInSstables(version, key);
    }

    /// <summary>Stands for "a live version exists" in <see cref="IsRowLive"/>; never read.</summary>
    private static readonly byte[] LiveMarker = [];

    private LatestRowLookup Lookup(PendingWrite? pending, TableState state, RowKey rowId, out byte[] value)
    {
        (byte[]? row, _) = ResolveRow(pending, state, rowId);
        if (row is null)
        {
            value = [];
            return LatestRowLookup.Missing;
        }

        if (ReferenceEquals(row, DeletedMarker))
        {
            value = [];
            return LatestRowLookup.Tombstone;
        }

        value = row;
        return LatestRowLookup.Live;
    }

    /// <summary>
    /// The newest version of a row: staged by this flow, in the unflushed overlay, or in an SSTable.
    /// Returns <see langword="null"/> when no version exists and <see cref="DeletedMarker"/> for a
    /// deletion. <c>Owned</c> is true when the bytes are a fresh copy the caller may keep or mutate.
    /// </summary>
    private (byte[]? Row, bool Owned) ResolveRow(PendingWrite? pending, TableState state, RowKey rowId, IStoredRowMaterializer? materializer = null)
    {
        if (pending is not null && pending.TryGetStaged(state, rowId, out byte[]? staged))
        {
            return (staged ?? DeletedMarker, false);
        }

        // The overlay is read before the file set: a flush publishes its file before evicting, so a
        // version evicted between the two reads is always in the file set taken afterwards.
        switch (state.LatestRows.TryGet(rowId, out byte[] overlay))
        {
            case LatestRowLookup.Live:
                return (overlay, false);
            case LatestRowLookup.Tombstone:
                return (DeletedMarker, false);
        }

        return (ReadFromSstables(state, rowId, materializer), true);
    }

    /// <summary>
    /// Searches level by level. Within a level the version with the highest sequence wins (level-0 files
    /// overlap); the first level holding the row decides, because compaction moves a whole level down,
    /// so every deeper level is older.
    /// </summary>
    private byte[]? ReadFromSstables(TableState state, RowKey rowId, IStoredRowMaterializer? materializer)
    {
        TableVersion version = state.AcquireVersion();
        try
        {
            SstableHit best = default;
            int bestLevel = -1;
            foreach (LsmSstableHandle file in version.Files)
            {
                int level = file.Metadata.Level;
                if (bestLevel >= 0 && level != bestLevel)
                {
                    break;
                }

                // Lookup starts with the range check and the Bloom probe; a miss costs only those.
                SstableHit hit = file.GetV2Reader(_readMode).Lookup(
                    rowId,
                    ulong.MaxValue,
                    (Materializer: materializer, Key: rowId),
                    static (type, sequence, value, read) => new SstableHit(
                        true,
                        sequence,
                        type == LsmValueType.Put ? Materialize(read.Materializer, read.Key, value) : DeletedMarker),
                    default(SstableHit));
                if (hit.Found && (!best.Found || hit.Sequence > best.Sequence))
                {
                    best = hit;
                    bestLevel = level;
                }
            }

            return best.Found ? best.Value : null;
        }
        finally
        {
            version.Release();
        }
    }

    private readonly record struct SstableHit(bool Found, ulong Sequence, byte[]? Value);

    /// <inheritdoc />
    public void DeleteRow(string databaseName, string tableName, RowKey rowId)
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
                pending.AddLiveDelta(state, -1);
            }
        }

        CompleteWrite(pending, ownsWrite);
    }

    bool IFixedWidthPatchStorageEngine.TryPatchFixedWidthRow(
        string databaseName,
        string tableName,
        RowKey rowId,
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
        CloseIndexTables(databaseName, tableName);
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
        CloseIndexTables(databaseName, tableName: null);
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
    public List<(RowKey NewRowId, byte[] RawRow)> CompactTable(string databaseName, string tableName) =>
        CompactTable(databaseName, tableName, materializer: null);

    /// <summary><see cref="CompactTable(string, string)"/> returning materialized rows.</summary>
    internal List<(RowKey NewRowId, byte[] RawRow)> CompactTable(string databaseName, string tableName, IStoredRowMaterializer? materializer)
    {
        TableState state = GetOrCreateTable(databaseName, tableName);

        lock (state.SyncRoot)
        {
            state.Table.FlushActiveMemTable();
            state.Table.CompactAllLevelsForMaintenance();
        }

        return CollectLiveRowsSorted(state, materializer);
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
        foreach (IndexTableState index in _indexTables.Values)
        {
            index.Dispose();
        }

        _indexTables.Clear();

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

    /// <summary>Test hook: runs while a frame is applied to the MemTables (after its LSN is assigned).</summary>
    internal Action? DuringApplyForTesting { get; set; }

    /// <summary>Test hook: overrides the MemTable rotation threshold for tables created afterwards.</summary>
    internal long? MemTableFlushThresholdOverrideBytes { get; set; }

    /// <summary>Diagnostics: row versions held in the managed-heap overlay (published, not yet flushed).</summary>
    internal long OverlayRowCount(string databaseName, string tableName) =>
        GetOrCreateTable(databaseName, tableName).LatestRows.Count;

    /// <summary>
    /// Test hook: stops like a killed process. Background flushes drain and files close, but MemTables
    /// are not flushed and the commit log is not pruned, so the next open must recover from the log.
    /// </summary>
    internal void SimulateCrashForTesting()
    {
        if (_disposed)
        {
            return;
        }

        // A killed process runs nothing after the kill. Background flushes still finishing here must not
        // prune the log: tables already abandoned (or removed from _tables) would no longer hold its
        // watermark back, so their unflushed frames would be cut. Prunes take the gate with TryEnter, so
        // holding it here cannot deadlock with a flush this shutdown waits for.
        lock (_pruneGate)
        {
            _crashed = true;
            SimulateCrashCore();
        }
    }

    private void SimulateCrashCore()
    {
        _disposed = true;
        foreach (TableState state in _tables.Values)
        {
            lock (state.SyncRoot)
            {
                state.Abandon();
            }
        }

        _tables.Clear();
        foreach (IndexTableState index in _indexTables.Values)
        {
            index.Abandon();
        }

        _indexTables.Clear();
        _commitLog.Dispose();
    }

    /// <summary>
    /// Test hook: writes a new version of a live row under the same row id (the storage step of an
    /// in-place UPDATE). Returns <see langword="false"/> when the row is not live.
    /// </summary>
    internal bool OverwriteRowForTesting(string databaseName, string tableName, RowKey rowId, byte[] rowBytes)
    {
        TableState state = GetOrCreateTable(databaseName, tableName);
        PendingWrite pending = AcquireWrite(out bool ownsWrite);
        bool live;
        lock (state.SyncRoot)
        {
            live = Lookup(pending, state, rowId, out _) == LatestRowLookup.Live;
            if (live)
            {
                pending.Stage(state, rowId, rowBytes);
                pending.Batch.Put(databaseName, tableName, rowId, rowBytes);
                pending.Touch(state, databaseName, tableName);
            }
        }

        CompleteWrite(pending, ownsWrite);
        return live;
    }

    /// <summary>Diagnostics: forces the table's MemTable into an SSTable now.</summary>
    internal void FlushTableForTesting(string databaseName, string tableName) =>
        GetOrCreateTable(databaseName, tableName).Table.FlushActiveMemTable();

    private TableState CreateTableState(string databaseName, string tableName)
    {
        string tableDirectory = GetTableDirectory(databaseName, tableName);
        Directory.CreateDirectory(tableDirectory);

        var manifest = new LsmManifest(Path.Combine(tableDirectory, "MANIFEST"));
        MigrateLegacyTableWal(tableDirectory, manifest);

        // No per-table WAL: durability comes from the shared commit log, and this table's MemTables
        // hold only mutations whose frames are already in it.
        var table = new LsmTable(tableDirectory, manifest) { Format = LsmSstableFormat.RowKeyed, ReadMode = _readMode };
        if (MemTableFlushThresholdOverrideBytes is long thresholdOverride)
        {
            table.FlushThresholdBytes = thresholdOverride;
        }

        var registry = new LsmFileRegistry(tableDirectory, manifest);
        table.CompactionRegistry = registry;
        table.BeforeFlushDurabilityBarrier = maxSequence => _commitLog.EnsureDurableThrough(checked((long)maxSequence));
        table.AfterFlush = PruneCommitLog;
        var state = new TableState(tableDirectory, manifest, table, registry, _readMode);
        table.AfterSstablePublished = memTable => EvictFlushed(state, memTable);

        UpgradeLegacySstables(state);
        LoadTableMetadata(state);
        return state;
    }

    /// <summary>
    /// Reads the table's next row id, live-row count, and flushed sequence from the SSTable properties
    /// (O(files), no row is read). Only files upgraded from v1 lack a live count; the rows are then
    /// counted once, and the next commit persists the count.
    /// </summary>
    private void LoadTableMetadata(TableState state)
    {
        long nextRowId = 1;
        long liveRowCount = 0;
        ulong metadataSequence = 0;
        bool hasMetadata = false;
        ulong flushed = 0;

        TableVersion version = state.AcquireVersion();
        try
        {
            foreach (LsmSstableHandle file in version.Files)
            {
                SsTableV2Properties properties = file.GetV2Reader(_readMode).Properties;
                flushed = Math.Max(flushed, Math.Max(properties.LargestSequence, properties.NextRowIdSequence));
                nextRowId = Math.Max(nextRowId, NextRowIdAfter(properties.LargestKey));
                if (properties.NextRowId > 0 && (!hasMetadata || properties.NextRowIdSequence >= metadataSequence))
                {
                    hasMetadata = true;
                    metadataSequence = properties.NextRowIdSequence;
                    nextRowId = Math.Max(nextRowId, properties.NextRowId);
                    liveRowCount = properties.LiveRowCount;
                }
            }
        }
        finally
        {
            version.Release();
        }

        if (version.Files.Length > 0 && (!hasMetadata || liveRowCount < 0))
        {
            liveRowCount = CollectLiveRowsSorted(state).Count;
        }

        state.NextRowId = Math.Max(state.NextRowId, nextRowId);
        state.LiveRowCount = liveRowCount;

        // Everything in SSTables is durable; only commit-log frames above this point still need replay.
        state.FlushedLsn = checked((long)flushed);
    }

    /// <summary>
    /// Rewrites v1 SSTables (one block per file, read by full scans only) as v2 once, so the read path
    /// only ever serves page-aligned, indexed files. Each file is replaced atomically through a manifest
    /// edit at the same level; a crash in between leaves either the old or the new file live.
    /// </summary>
    /// <remarks>
    /// v1 metadata has no live-row count. The upgrade counts live rows across all files first and stores
    /// the count in the file that holds the newest metadata version, so later opens never recount.
    /// </remarks>
    private void UpgradeLegacySstables(TableState state)
    {
        var legacy = new List<LsmTableFileMetadata>();
        for (int level = 0; level <= MaxLevelToScan; level++)
        {
            foreach (LsmTableFileMetadata file in state.FileRegistry.ExecuteVersionEdit(() => state.Manifest.GetLiveFiles(level)))
            {
                if (!SsTableV2Format.IsV2File(Path.Combine(state.TableDirectory, file.FileName)))
                {
                    legacy.Add(file);
                }
            }
        }

        if (legacy.Count == 0)
        {
            return;
        }

        // Pass 1: newest version per row across every live file, and where the newest metadata lives.
        var newest = new Dictionary<RowKey, (ulong Sequence, bool Live)>();
        long metadataFile = -1;
        ulong metadataSequence = 0;
        bool sawMetadata = false;
        void Observe(RowKey rowId, ulong sequence, bool live)
        {
            if (!newest.TryGetValue(rowId, out var current) || sequence > current.Sequence)
            {
                newest[rowId] = (sequence, live);
            }
        }

        foreach (LsmTableFileMetadata file in AllLiveFiles(state))
        {
            string path = Path.Combine(state.TableDirectory, file.FileName);
            if (SsTableV2Format.IsV2File(path))
            {
                using SsTableV2Reader reader = SsTableV2Reader.Open(path, LsmReadMode.PositionalRead);
                SsTableV2Cursor cursor = reader.OpenCursor();
                while (cursor.MoveNext())
                {
                    Observe(cursor.Key, cursor.Sequence, cursor.Type == LsmValueType.Put);
                }

                if (reader.Properties.NextRowId > 0 && (!sawMetadata || reader.Properties.NextRowIdSequence >= metadataSequence))
                {
                    sawMetadata = true;
                    metadataSequence = reader.Properties.NextRowIdSequence;
                    metadataFile = -1; // a v2 file cannot be rewritten here; open recounts in that rare case
                }

                continue;
            }

            byte[] bytes = File.ReadAllBytes(path);
            SsTableReader.Load(bytes);
            foreach (SstableEntry entry in ReadSstableEntries(bytes))
            {
                ReadOnlySpan<byte> userKey = InternalKey.UserKey(entry.InternalKey);
                ulong sequence = InternalKey.Sequence(entry.InternalKey);
                if (LsmRowKeyCodec.IsRowKey(userKey))
                {
                    Observe(LsmRowKeyCodec.DecodeRowKey(userKey), sequence, InternalKey.ValueType(entry.InternalKey) == LsmValueType.Put);
                }
                else if (LsmRowKeyCodec.IsTableMetadataKey(userKey) && (!sawMetadata || sequence >= metadataSequence))
                {
                    sawMetadata = true;
                    metadataSequence = sequence;
                    metadataFile = file.FileNumber;
                }
            }
        }

        long liveRows = newest.Values.Count(static version => version.Live);
        newest.Clear();

        // Pass 2: rewrite each legacy file.
        foreach (LsmTableFileMetadata file in legacy)
        {
            byte[] bytes = File.ReadAllBytes(Path.Combine(state.TableDirectory, file.FileName));
            List<SstableEntry> entries = ReadSstableEntries(bytes);
            byte[]? first = null;
            byte[]? last = null;
            foreach (SstableEntry entry in entries)
            {
                ReadOnlySpan<byte> rowUserKey = InternalKey.UserKey(entry.InternalKey);
                if (LsmRowKeyCodec.IsRowKey(rowUserKey))
                {
                    byte[] widened = LsmRowKeyCodec.RowInternalKey(
                        LsmRowKeyCodec.DecodeRowKey(rowUserKey),
                        InternalKey.Sequence(entry.InternalKey),
                        InternalKey.ValueType(entry.InternalKey));
                    first ??= widened;
                    last = widened;
                }
            }

            if (first is null || last is null)
            {
                throw new InvalidDataException($"Legacy SSTable '{file.FileName}' holds no rows and cannot be upgraded.");
            }

            long fileNumber = state.Manifest.AllocateFileNumber();
            string fileName = $"{fileNumber:D6}.sst";
            long length = 0;
            bool holdsNewestMetadata = file.FileNumber == metadataFile;
            LsmDurableFileOperations.WriteFileAtomically(
                Path.Combine(state.TableDirectory, fileName),
                overwrite: false,
                stream =>
                {
                    using var writer = new SsTableV2Writer(stream, alignment: 0, entries.Count);
                    long nextRowId = 0;
                    foreach (SstableEntry entry in entries)
                    {
                        LsmRowKeyCodec.AddInternalEntry(writer, entry.InternalKey, entry.Value);
                        if (holdsNewestMetadata
                            && LsmRowKeyCodec.IsTableMetadataKey(InternalKey.UserKey(entry.InternalKey))
                            && InternalKey.Sequence(entry.InternalKey) == metadataSequence)
                        {
                            nextRowId = LsmRowKeyCodec.DecodeTableMetadata(entry.Value).NextRowId;
                        }
                    }

                    if (holdsNewestMetadata && nextRowId > 0)
                    {
                        writer.SetTableMetadata(nextRowId, liveRows, metadataSequence);
                    }

                    length = writer.Finish();
                },
                crashHook: null,
                LsmCrashPoint.AfterSstableTempFileFsyncBeforeRename,
                LsmCrashPoint.AfterSstableRenameBeforeDirectoryFsync,
                LsmCrashPoint.AfterSstableDirectoryFsyncBeforeManifest);

            var upgraded = new LsmTableFileMetadata(fileNumber, file.Level, first, last, length, fileName);
            var edit = new LsmVersionEdit();
            edit.DeleteFile(file.Level, file.FileNumber);
            edit.AddFile(upgraded);
            state.FileRegistry.ExecuteVersionEdit(() =>
            {
                state.Manifest.ApplyEdit(edit);
                state.FileRegistry.Register(upgraded);
                state.FileRegistry.MarkDeleted([file]);
                return true;
            });
        }
    }

    private static List<LsmTableFileMetadata> AllLiveFiles(TableState state) =>
        state.FileRegistry.ExecuteVersionEdit(() =>
        {
            var files = new List<LsmTableFileMetadata>();
            for (int level = 0; level <= MaxLevelToScan; level++)
            {
                files.AddRange(state.Manifest.GetLiveFiles(level));
            }

            return files;
        });

    /// <summary>
    /// Drops overlay versions that a flushed MemTable wrote to its SSTable. Runs on the flushing thread
    /// after the file is in the manifest; works in chunks so publishers are never stalled for long.
    /// </summary>
    private static void EvictFlushed(TableState state, MemTable memTable)
    {
        if (memTable.MaxSequence is not ulong flushedThrough)
        {
            return;
        }

        const int ChunkSize = 1024;
        int inChunk = 0;
        Monitor.Enter(state.OverlayGate);
        try
        {
            foreach (MemTableEntry entry in memTable)
            {
                ReadOnlySpan<byte> userKey = InternalKey.UserKey(entry.InternalKey);
                if (LsmRowKeyCodec.IsRowKey(userKey))
                {
                    state.LatestRows.EvictFlushed(LsmRowKeyCodec.DecodeRowKey(userKey), flushedThrough);
                }

                if (++inChunk == ChunkSize)
                {
                    inChunk = 0;
                    Monitor.Exit(state.OverlayGate);
                    Monitor.Enter(state.OverlayGate);
                }
            }
        }
        finally
        {
            Monitor.Exit(state.OverlayGate);
        }
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

    /// <summary>
    /// Opens every table directory on disk, so the LSN floor covers all persisted sequence numbers,
    /// including the watermarks of vector index snapshots.
    /// </summary>
    private void OpenExistingTables()
    {
        long floor = 0;
        foreach (string databaseDirectory in Directory.EnumerateDirectories(_storageDirectory))
        {
            foreach (string tableDirectory in Directory.EnumerateDirectories(databaseDirectory))
            {
                string databaseName = Path.GetFileName(databaseDirectory);
                string tableName = Path.GetFileName(tableDirectory);

                // Index tables count toward the LSN floor even when the row table has no files yet: new
                // commits must get LSNs above everything any key space already flushed.
                floor = Math.Max(floor, OpenExistingIndexTables(databaseName, tableName, tableDirectory));
                floor = Math.Max(floor, HnswSnapshotFile.MaxWatermark(tableDirectory));
                if (File.Exists(Path.Combine(tableDirectory, "MANIFEST")))
                {
                    floor = Math.Max(floor, GetOrCreateTable(databaseName, tableName).FlushedLsn);
                }
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
            var hold = new FrameHold();
            try
            {
                ReplayFrame(frame, hold, userKey);
            }
            finally
            {
                hold.Release();
            }
        }
    }

    private void ReplayFrame(LsmCommitFrame frame, FrameHold hold, Span<byte> userKey)
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

                case LsmCommitOperationKind.IndexPut or LsmCommitOperationKind.IndexDelete:
                    ApplyIndexOperation(operation, sequence, replaying: true, hold);
                    continue;
            }

            TableState state = GetOrCreateTable(operation.Database, operation.Table);
            if (frame.Lsn <= state.FlushedLsn)
            {
                continue;
            }

            hold.Hold(state.Table);
            bool wasLive = ResolveRow(null, state, operation.RowId).Row is { } previous
                && !ReferenceEquals(previous, DeletedMarker);
            LsmRowKeyCodec.EncodeRowKey(operation.RowId, userKey);
            if (operation.Kind == LsmCommitOperationKind.Put)
            {
                state.Table.Put(userKey, sequence, operation.Value);
                lock (state.OverlayGate)
                {
                    state.LatestRows.Set(operation.RowId, operation.Value, sequence);
                }

                if (!wasLive)
                {
                    state.LiveRowCount++;
                }
            }
            else
            {
                state.Table.Delete(userKey, sequence);
                lock (state.OverlayGate)
                {
                    state.LatestRows.SetTombstone(operation.RowId, sequence);
                }

                if (wasLive)
                {
                    state.LiveRowCount--;
                }
            }

            state.NextRowId = Math.Max(state.NextRowId, NextRowIdAfter(operation.RowId));

            touched.Add(state);
        }

        foreach (TableState state in touched)
        {
            hold.Hold(state.Table);
            state.Table.Put(NextRowIdMetadataKey, sequence, LsmRowKeyCodec.EncodeTableMetadata(state.NextRowId, state.LiveRowCount));
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

            // Collapsing builds a map over every operation; only a write unit that touched some row or
            // index entry twice has anything to collapse (bulk inserts never do).
            if (pending.HasRepeatedWrites)
            {
                pending.Batch.CollapseToLatestPerRow();
            }

            long? assignedLsn = null;
            bool durable = false;
            try
            {
                LsmCommitTicket ticket;
                lock (_commitGate)
                {
                    ticket = _commitLog.Append(pending.Batch);
                    assignedLsn = ticket.Lsn;
                    ApplyToMemTables(pending, checked((ulong)ticket.Lsn));
                    Volatile.Write(ref _appliedLsn, ticket.Lsn);
                }

                ticket.Wait();
                durable = true;
            }
            finally
            {
                // Every assigned LSN must take its publication turn, even when the commit failed after the
                // LSN was assigned: later commits wait for it. Rows, indexes, and notifications become
                // visible only for a durable commit, and strictly in commit order.
                if (assignedLsn is long lsn)
                {
                    PublishInOrder(lsn, durable ? () => PublishStaged(pending, checked((ulong)lsn)) : null);
                }
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

    private void PublishStaged(PendingWrite pending, ulong sequence)
    {
        foreach ((TableState state, Dictionary<RowKey, byte[]?> rows) in pending.StagedTables)
        {
            lock (state.SyncRoot)
            lock (state.OverlayGate)
            {
                foreach ((RowKey rowId, byte[]? value) in rows)
                {
                    if (value is null)
                    {
                        state.LatestRows.SetTombstone(rowId, sequence);
                    }
                    else
                    {
                        state.LatestRows.Set(rowId, value, sequence);
                    }
                }
            }
        }

        PublishStagedIndexes(pending, sequence);
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
                // Backstop: a broken invariant must surface as an error, not as a silent engine-wide hang.
                if (!Monitor.Wait(_publishGate, PublicationTimeout))
                {
                    throw new StorageException(
                        $"Commit publication stalled: LSN {lsn} waited {PublicationTimeout.TotalSeconds:N0} s for LSN {_nextPublishLsn}.");
                }
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
        DuringApplyForTesting?.Invoke();
        var frame = new FrameHold();
        try
        {
            ApplyFrame(pending, sequence, frame);
        }
        finally
        {
            frame.Release();
        }
    }

    private void ApplyFrame(PendingWrite pending, ulong sequence, FrameHold frame)
    {
        Span<byte> userKey = stackalloc byte[UserKeySize];
        IReadOnlyList<LsmCommitOperation> operations = pending.Batch.Operations;

        // A MemTable insert is cheapest right after the previous key. Keys of one frame are distinct and
        // the frame applies as a whole, so their order is free: apply them in key order when they are not
        // already in it (time-ordered GUIDs are ordered only to the millisecond).
        int[]? order = RowOperationsOutOfOrder(operations) ? SortRowOperations(operations) : null;
        TableState? target = null;
        try
        {
            for (int n = 0; n < operations.Count; n++)
            {
                LsmCommitOperation operation = operations[order is null ? n : order[n]];
                if (operation.Kind is LsmCommitOperationKind.IndexPut or LsmCommitOperationKind.IndexDelete)
                {
                    continue; // applied below from the staged changes, already decoded and collapsed
                }

                if (target is null
                    || !ReferenceEquals(operation.Table, target.TableName)
                    || !ReferenceEquals(operation.Database, target.DatabaseName))
                {
                    if (!_tables.TryGetValue((operation.Database, operation.Table), out target))
                    {
                        target = null;
                        continue; // dropped while the statement was running
                    }

                    target.DatabaseName = operation.Database;
                    target.TableName = operation.Table;
                }

                frame.Hold(target.Table);
                LsmRowKeyCodec.EncodeRowKey(operation.RowId, userKey);
                if (operation.Kind == LsmCommitOperationKind.Put)
                {
                    target.Table.Put(userKey, sequence, operation.Value);
                }
                else
                {
                    target.Table.Delete(userKey, sequence);
                }
            }
        }
        finally
        {
            if (order is not null)
            {
                ArrayPool<int>.Shared.Return(order);
            }
        }

        // The staged index changes hold exactly the batch's index operations (last change per entry wins,
        // like the batch collapse), with keys already built: no per-operation decoding.
        foreach (KeyValuePair<IndexTableState, Dictionary<byte[], bool>> index in pending.StagedIndexes)
        {
            if (!_indexTables.TryGetValue((index.Key.Database, index.Key.TableName, index.Key.Index), out IndexTableState? live)
                || !ReferenceEquals(live, index.Key))
            {
                continue; // dropped while the statement was running
            }

            frame.Hold(index.Key.Table);
            foreach (KeyValuePair<byte[], bool> change in index.Value)
            {
                if (change.Value)
                {
                    index.Key.Table.Put(change.Key, sequence, ReadOnlySpan<byte>.Empty);
                }
                else
                {
                    index.Key.Table.Delete(change.Key, sequence);
                }
            }
        }

        foreach ((TableState state, _, _) in pending.TouchedTables)
        {
            // Under the commit gate, in LSN order: the persisted count is the count as of this frame.
            state.LiveRowCount += pending.LiveDeltaFor(state);
            frame.Hold(state.Table);
            state.Table.Put(NextRowIdMetadataKey, sequence, LsmRowKeyCodec.EncodeTableMetadata(state.ReadNextRowId(), state.LiveRowCount));
        }
    }

    private static bool RowOperationsOutOfOrder(IReadOnlyList<LsmCommitOperation> operations)
    {
        for (int i = 1; i < operations.Count; i++)
        {
            LsmCommitOperation previous = operations[i - 1];
            LsmCommitOperation current = operations[i];
            if (current.RowId < previous.RowId
                && string.Equals(current.Table, previous.Table, StringComparison.Ordinal)
                && string.Equals(current.Database, previous.Database, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Operation positions ordered by table, then row key (a pooled array; return it).</summary>
    private static int[] SortRowOperations(IReadOnlyList<LsmCommitOperation> operations)
    {
        int count = operations.Count;
        int[] order = ArrayPool<int>.Shared.Rent(count);
        bool oneTable = true;
        for (int i = 0; i < count; i++)
        {
            order[i] = i;
            oneTable &= i == 0
                || (string.Equals(operations[i].Table, operations[0].Table, StringComparison.Ordinal)
                    && string.Equals(operations[i].Database, operations[0].Database, StringComparison.Ordinal));
        }

        if (!oneTable)
        {
            Array.Sort(order, 0, count, new OperationKeyOrder(operations));
            return order;
        }

        // One table (the usual frame): sort the 16-byte keys themselves, carrying the positions along.
        RowKey[] keys = ArrayPool<RowKey>.Shared.Rent(count);
        try
        {
            for (int i = 0; i < count; i++)
            {
                keys[i] = operations[i].RowId;
            }

            Array.Sort(keys, order, 0, count);
        }
        finally
        {
            ArrayPool<RowKey>.Shared.Return(keys);
        }

        return order;
    }

    private sealed class OperationKeyOrder(IReadOnlyList<LsmCommitOperation> operations) : IComparer<int>
    {
        public int Compare(int x, int y)
        {
            LsmCommitOperation left = operations[x];
            LsmCommitOperation right = operations[y];
            int byDatabase = string.CompareOrdinal(left.Database, right.Database);
            if (byDatabase != 0)
            {
                return byDatabase;
            }

            int byTable = string.CompareOrdinal(left.Table, right.Table);
            return byTable != 0 ? byTable : left.RowId.CompareTo(right.RowId);
        }
    }

    /// <summary>The tables one commit frame touches, held so none rotates or flushes mid-frame.</summary>
    private sealed class FrameHold
    {
        private readonly List<LsmTable> _tables = new(2);

        public void Hold(LsmTable table)
        {
            foreach (LsmTable held in _tables)
            {
                if (ReferenceEquals(held, table))
                {
                    return;
                }
            }

            table.BeginFrame();
            _tables.Add(table);
        }

        public void Release()
        {
            foreach (LsmTable table in _tables)
            {
                table.EndFrame();
            }

            _tables.Clear();
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

        if (_crashed)
        {
            Monitor.Exit(_pruneGate);
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

            // Index entries ride in the same frames: a frame stays until its index changes are flushed too.
            foreach (IndexTableState index in _indexTables.Values)
            {
                if (index.Table.OldestUnflushedSequence is ulong oldest)
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

    /// <summary>Every live row in key order (see <see cref="MergeLiveRows{TState}"/>).</summary>
    private List<(RowKey RowId, byte[] RawRow)> CollectLiveRowsSorted(TableState state, IStoredRowMaterializer? materializer = null)
    {
        var rows = new List<(RowKey RowId, byte[] RawRow)>((int)Math.Min(Math.Max(state.LiveRowCount, 0), int.MaxValue));
        MergeLiveRows(state, (Rows: rows, Materializer: materializer), static (target, rowId, _, value) =>
        {
            target.Rows.Add((rowId, Materialize(target.Materializer, rowId, value)));
            return true;
        });
        return rows;
    }

    /// <summary>
    /// Visits the key and version sequence of every live row of a table in key order, without copying
    /// any row. A row whose sequence is at most a known commit LSN has not changed since that commit.
    /// </summary>
    public void ForEachLiveRowVersion(string databaseName, string tableName, Action<RowKey, ulong> visit)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(visit);
        MergeLiveRows(GetOrCreateTable(databaseName, tableName), visit, static (callback, rowId, sequence, _) =>
        {
            callback(rowId, sequence);
            return true;
        });
    }

    /// <summary>Visits one live row of a scan; the stored bytes are only valid during the call. Return false to stop.</summary>
    internal delegate bool StoredRowScanVisitor<TState>(ref TState state, RowKey rowId, ReadOnlySpan<byte> storedRow);

    /// <summary>
    /// Streams the live rows with keys in <c>[lower, upper)</c> (a null bound is open) in key order, without
    /// copying them. Rows are passed in their stored form: a clustered table's key cell is NULL and the
    /// row key carries the value. Only valid outside a write flow (staged writes are not merged).
    /// </summary>
    internal void ScanStoredRows<TState>(string databaseName, string tableName, RowKey? lower, RowKey? upper, ref TState state, StoredRowScanVisitor<TState> visit)
    {
        var box = new ScanBox<TState>(state, visit);
        MergeLiveRows(GetOrCreateTable(databaseName, tableName), box, static (b, rowId, _, value) => b.Visit(b, rowId, value), lower, upper);
        state = box.State;
    }

    /// <summary>Whether this flow has staged writes to the table that a scan would have to merge (a scan then takes the general path).</summary>
    internal bool HasStagedWrites(string databaseName, string tableName) =>
        _ambientWrite.Value?.StagedFor(GetOrCreateTable(databaseName, tableName)) is { Count: > 0 };

    private sealed class ScanBox<TState>(TState state, StoredRowScanVisitor<TState> visitor)
    {
        public TState State = state;

        public bool Visit(ScanBox<TState> self, RowKey rowId, ReadOnlySpan<byte> value) => visitor(ref self.State, rowId, value);
    }

    /// <summary>Highest commit LSN applied to the in-memory tables; every row sequence is at most this.</summary>
    internal long AppliedLsn => Volatile.Read(ref _appliedLsn);

    private delegate bool LiveRowVisitor<TState>(TState state, RowKey rowId, ulong sequence, ReadOnlySpan<byte> value);

    /// <summary>
    /// Every live row in key order: a k-way merge of the unflushed overlay and the SSTable cursors
    /// (newest source wins per row id, deletions suppress). The overlay is captured before the file set
    /// for the same reason as in <see cref="ResolveRow"/>. The value span is only valid during the call.
    /// </summary>
    private void MergeLiveRows<TState>(TableState state, TState visitorState, LiveRowVisitor<TState> visit, RowKey? lower = null, RowKey? upper = null)
    {
        bool bounded = lower is not null || upper is not null;
        var overlay = new List<(RowKey RowId, ulong Sequence, byte[]? Value)>(bounded ? 16 : (int)Math.Min(state.LatestRows.Count, int.MaxValue));
        state.LatestRows.ForEachVersion(overlay, static (list, rowId, sequence, value) => list.Add((rowId, sequence, value)), lower, upper);

        TableVersion version = state.AcquireVersion();
        try
        {
            var cursors = new SsTableV2Cursor[version.Files.Length];
            var hasCurrent = new bool[cursors.Length];
            for (int i = 0; i < cursors.Length; i++)
            {
                cursors[i] = version.Files[i].GetV2Reader(_readMode).OpenCursor();
                hasCurrent[i] = lower is { } from ? cursors[i].SeekTo(from) : cursors[i].MoveNext();
                if (hasCurrent[i] && upper is { } to && cursors[i].Key >= to)
                {
                    hasCurrent[i] = false;
                }
            }

            int overlayIndex = 0;
            while (true)
            {
                RowKey next = default;
                bool any = false;
                if (overlayIndex < overlay.Count)
                {
                    next = overlay[overlayIndex].RowId;
                    any = true;
                }

                for (int i = 0; i < cursors.Length; i++)
                {
                    if (hasCurrent[i] && (!any || cursors[i].Key < next))
                    {
                        next = cursors[i].Key;
                        any = true;
                    }
                }

                if (!any)
                {
                    break;
                }

                // Sources in priority order: overlay, then files newest first; the first holder wins.
                bool decided = false;
                if (overlayIndex < overlay.Count && overlay[overlayIndex].RowId == next)
                {
                    (_, ulong sequence, byte[]? value) = overlay[overlayIndex];
                    if (value is not null && !visit(visitorState, next, sequence, value))
                    {
                        return;
                    }

                    decided = true;
                    overlayIndex++;
                }

                // Otherwise the file version with the highest sequence wins (each cursor sits on the
                // newest version of the row id in its file).
                int winner = -1;
                for (int i = 0; !decided && i < cursors.Length; i++)
                {
                    if (hasCurrent[i] && cursors[i].Key == next && (winner < 0 || cursors[i].Sequence > cursors[winner].Sequence))
                    {
                        winner = i;
                    }
                }

                if (winner >= 0 && cursors[winner].Type == LsmValueType.Put
                    && !visit(visitorState, next, cursors[winner].Sequence, cursors[winner].Value))
                {
                    return;
                }

                for (int i = 0; i < cursors.Length; i++)
                {
                    // Skip every version of this row key in this file.
                    while (hasCurrent[i] && cursors[i].Key == next)
                    {
                        hasCurrent[i] = cursors[i].MoveNext();
                    }

                    if (hasCurrent[i] && upper is { } to && cursors[i].Key >= to)
                    {
                        hasCurrent[i] = false;
                    }
                }
            }
        }
        finally
        {
            version.Release();
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

    /// <summary>
    /// The engine row id that must follow <paramref name="key"/>: one past it for a positive integer key,
    /// otherwise 1 (negative integers and GUIDs are never engine-assigned, so they reserve nothing).
    /// </summary>
    private static long NextRowIdAfter(RowKey key) =>
        key.IsInt64 && key.AsInt64() is > 0 and < long.MaxValue ? key.AsInt64() + 1 : 1;

    private sealed class PendingWrite
    {
        private readonly List<(TableState State, string Database, string Table)> _touched = [];

        private readonly Dictionary<TableState, Dictionary<RowKey, byte[]?>> _staged = []; // TableState uses reference equality

        // Change in live rows per table, relative to what this flow saw when it staged each write.
        private readonly Dictionary<TableState, long> _liveDelta = [];

        // Index changes of this write unit: user key (key prefix and row id) → live.
        private readonly Dictionary<IndexTableState, Dictionary<byte[], bool>> _stagedIndexes = [];

        public IEnumerable<KeyValuePair<IndexTableState, Dictionary<byte[], bool>>> StagedIndexes => _stagedIndexes;

        public void StageIndex(IndexTableState state, byte[] userKey, bool live)
        {
            if (!_stagedIndexes.TryGetValue(state, out Dictionary<byte[], bool>? changes))
            {
                changes = new Dictionary<byte[], bool>(ByteArrayComparer.Instance);
                _stagedIndexes[state] = changes;
            }

            HasRepeatedWrites |= changes.ContainsKey(userKey);
            changes[userKey] = live;
        }

        /// <summary>Whether some row or index entry was written more than once (the batch then needs collapsing).</summary>
        public bool HasRepeatedWrites { get; private set; }

        /// <summary>Makes room for <paramref name="rows"/> more staged rows of one table, so a bulk write does not regrow.</summary>
        public void Reserve(TableState state, int rows)
        {
            if (!_staged.TryGetValue(state, out Dictionary<RowKey, byte[]?>? staged))
            {
                staged = new Dictionary<RowKey, byte[]?>(rows);
                _staged[state] = staged;
            }
            else
            {
                staged.EnsureCapacity(staged.Count + rows);
            }

            Batch.Reserve(rows);
        }

        public Dictionary<byte[], bool>? StagedIndexFor(IndexTableState state) =>
            _stagedIndexes.TryGetValue(state, out Dictionary<byte[], bool>? changes) ? changes : null;

        public void AddLiveDelta(TableState state, long delta) =>
            _liveDelta[state] = (_liveDelta.TryGetValue(state, out long current) ? current : 0) + delta;

        public long LiveDeltaFor(TableState state) => _liveDelta.TryGetValue(state, out long delta) ? delta : 0;

        public LsmCommitBatch Batch { get; } = new();

        /// <summary>In-memory effects (indexes, MVCC, notifications, lock releases) held until publication.</summary>
        public DeferredPublication Publication { get; } = new();

        public IEnumerable<KeyValuePair<TableState, Dictionary<RowKey, byte[]?>>> StagedTables => _staged;

        /// <summary>Records a row's pending version (<see langword="null"/> = deleted) for publication.</summary>
        public void Stage(TableState state, RowKey rowId, byte[]? value)
        {
            if (!_staged.TryGetValue(state, out Dictionary<RowKey, byte[]?>? rows))
            {
                rows = [];
                _staged[state] = rows;
            }

            if (!rows.TryAdd(rowId, value))
            {
                rows[rowId] = value;
                HasRepeatedWrites = true;
            }
        }

        public bool TryGetStaged(TableState state, RowKey rowId, out byte[]? value)
        {
            value = null;
            return _staged.TryGetValue(state, out Dictionary<RowKey, byte[]?>? rows) && rows.TryGetValue(rowId, out value);
        }

        public IReadOnlyDictionary<RowKey, byte[]?>? StagedFor(TableState state) =>
            _staged.TryGetValue(state, out Dictionary<RowKey, byte[]?>? rows) ? rows : null;

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
            _liveDelta.Clear();
            _stagedIndexes.Clear();
            HasRepeatedWrites = false;
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

    /// <summary>
    /// What row tables and index tables share: the manifest, file registry, and MemTable owner of one
    /// key space, the cached file set (version) readers search, and an orderly shutdown.
    /// </summary>
    private abstract class LsmKeySpaceState : IDisposable
    {
        private TableVersion? _version;

        protected LsmKeySpaceState(string directory, LsmManifest manifest, LsmTable table, LsmFileRegistry fileRegistry)
        {
            TableDirectory = directory;
            Manifest = manifest;
            Table = table;
            FileRegistry = fileRegistry;
        }

        public string TableDirectory { get; }

        public LsmManifest Manifest { get; }

        public LsmTable Table { get; }

        public LsmFileRegistry FileRegistry { get; }

        /// <summary>
        /// Serializes overlay publishes and flush evictions. Never held while calling into the table, so a
        /// background flush can always take it (CompactTable holds SyncRoot while waiting for the flush).
        /// </summary>
        public object OverlayGate { get; } = new();

        /// <summary>Highest sequence number already durable in this key space's SSTables when it was opened.</summary>
        public long FlushedLsn { get; set; }

        /// <summary>
        /// The current set of live SSTables, with a reference held for the caller. One volatile read of the
        /// manifest stamp decides whether the cached set is current; it is rebuilt under the file
        /// registry's version gate (the same gate flush and compaction edit the manifest under).
        /// </summary>
        public TableVersion AcquireVersion()
        {
            while (true)
            {
                TableVersion? current = Volatile.Read(ref _version);
                if (current is not null && current.Stamp == Manifest.ChangeStamp && current.TryAcquire())
                {
                    return current;
                }

                RefreshVersion();
            }
        }

        private void RefreshVersion()
        {
            FileRegistry.ExecuteVersionEdit(() =>
            {
                long stamp = Manifest.ChangeStamp;
                TableVersion? current = _version;
                if (current is not null && current.Stamp == stamp && current.IsLive)
                {
                    return false;
                }

                // Grouped by level, shallowest first; within a level the highest sequence wins.
                var files = new List<LsmTableFileMetadata>();
                files.AddRange(Manifest.GetLiveFiles(0).OrderByDescending(static file => file.FileNumber));
                for (int level = 1; level <= MaxLevelToScan; level++)
                {
                    files.AddRange(Manifest.GetLiveFiles(level));
                }

                var next = new TableVersion(stamp, FileRegistry.AddRefs(files), FileRegistry);
                Volatile.Write(ref _version, next);
                current?.Release();
                return true;
            });
        }

        public virtual void Dispose()
        {
            Table.FlushActiveMemTable();
            Abandon();
        }

        /// <summary>Closes without flushing the active MemTable (a crash, for tests).</summary>
        public void Abandon()
        {
            Table.Dispose();

            // Drop the cached file set and unmap every file, so the directory can be deleted (Windows).
            Interlocked.Exchange(ref _version, null)?.Release();
            FileRegistry.CloseReaders();
        }
    }

    private sealed class TableState(string tableDirectory, LsmManifest manifest, LsmTable table, LsmFileRegistry fileRegistry, LsmReadMode readMode)
        : LsmKeySpaceState(tableDirectory, manifest, table, fileRegistry)
    {
        private long _nextRowId = 1;
        private long _liveRowCount;

        /// <summary>The names commit operations carry for this table (cached to skip a lookup per row).</summary>
        public string? DatabaseName { get; set; }

        public string? TableName { get; set; }

        public object SyncRoot { get; } = new();

        public LsmReadMode ReadMode { get; } = readMode;

        /// <summary>Live rows as of the last applied commit (updated in LSN order, persisted with each commit).</summary>
        public long LiveRowCount
        {
            get => Volatile.Read(ref _liveRowCount);
            set => Volatile.Write(ref _liveRowCount, value);
        }

        /// <summary>
        /// Published row versions not yet flushed to an SSTable (flushed versions are evicted and read from
        /// the mapped files). Read lock-free; mutated under <see cref="LsmKeySpaceState.OverlayGate"/>.
        /// </summary>
        public LsmLatestRowStore LatestRows { get; } = new();

        public long NextRowId
        {
            get => Volatile.Read(ref _nextRowId);
            set => Volatile.Write(ref _nextRowId, value);
        }

        public long ReadNextRowId() => Volatile.Read(ref _nextRowId);
    }

    /// <summary>
    /// An immutable set of live SSTables in search order, reference-counted so a reader can finish with a
    /// file that compaction has already replaced; the files' own references are released (and replaced
    /// files deleted) when the last reader is done.
    /// </summary>
    private sealed class TableVersion(long stamp, LsmSstableHandle[] files, LsmFileRegistry registry)
    {
        private int _references = 1; // the owning TableState's reference

        public long Stamp { get; } = stamp;

        public LsmSstableHandle[] Files { get; } = files;

        public bool IsLive => Volatile.Read(ref _references) > 0;

        public bool TryAcquire()
        {
            int current;
            do
            {
                current = Volatile.Read(ref _references);
                if (current == 0)
                {
                    return false;
                }
            }
            while (Interlocked.CompareExchange(ref _references, current + 1, current) != current);

            return true;
        }

        public void Release()
        {
            if (Interlocked.Decrement(ref _references) == 0)
            {
                registry.Release(Files);
            }
        }
    }


    private readonly record struct SstableEntry(byte[] InternalKey, byte[] Value);
}
