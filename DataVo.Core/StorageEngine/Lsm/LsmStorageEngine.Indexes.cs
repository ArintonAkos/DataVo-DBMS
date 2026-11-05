using System.Collections.Concurrent;
using DataVo.Core.StorageEngine.Lsm.V2;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>
/// Persisted secondary and primary-key indexes: each index is its own key space ("index table") of
/// entries <c>(key, row id)</c> stored in keyed (v3) SSTables next to its table. Index changes travel in
/// the same commit frame as the row changes, so rows and indexes survive a crash together, and opening
/// a database no longer rebuilds indexes by decoding every row.
/// </summary>
public sealed partial class LsmStorageEngine
{
    /// <summary>Directory (inside a table's directory) holding one subdirectory per index table.</summary>
    internal const string IndexDirectoryName = ".idx";

    private const string IndexBuiltMarker = "BUILT";

    private readonly ConcurrentDictionary<(string Database, string Table, string Index), IndexTableState> _indexTables = new();

    /// <summary>Whether the index has been fully built (bulk build or creation on an empty table).</summary>
    internal bool IsIndexBuilt(string databaseName, string tableName, string indexName) =>
        File.Exists(Path.Combine(GetIndexDirectory(databaseName, tableName, indexName), IndexBuiltMarker));

    /// <summary>Stages index entry (key, row id) in the current write unit.</summary>
    internal void IndexPut(string databaseName, string tableName, string indexName, byte[] encodedKey, long rowId) =>
        StageIndexChange(databaseName, tableName, indexName, encodedKey, rowId, live: true);

    /// <summary>Stages the removal of index entry (key, row id) in the current write unit.</summary>
    internal void IndexDelete(string databaseName, string tableName, string indexName, byte[] encodedKey, long rowId) =>
        StageIndexChange(databaseName, tableName, indexName, encodedKey, rowId, live: false);

    /// <summary>
    /// Row ids indexed under <paramref name="encodedKey"/>, ascending: this flow's staged changes, then
    /// the published overlay, then the index runs, newest version per row id winning.
    /// </summary>
    internal List<long> IndexLookup(string databaseName, string tableName, string indexName, byte[] encodedKey)
    {
        IndexTableState state = GetOrCreateIndexTable(databaseName, tableName, indexName);
        byte[] prefix = LsmIndexKeyCodec.KeyPrefix(encodedKey);
        var newest = new Dictionary<long, (ulong Sequence, bool Live)>();

        // Overlay before the file set: a flush publishes its file before evicting (see ResolveRow).
        lock (state.OverlayGate)
        {
            if (state.Overlay.TryGetValue(prefix, out Dictionary<long, (ulong Sequence, bool Live)>? published))
            {
                foreach (KeyValuePair<long, (ulong Sequence, bool Live)> entry in published)
                {
                    newest[entry.Key] = entry.Value;
                }
            }
        }

        TableVersion version = state.AcquireVersion();
        try
        {
            foreach (LsmSstableHandle file in version.Files)
            {
                file.GetKeyedReader(_readMode).ForEachWithPrefix(prefix, newest, static (userKey, sequence, type, _, versions) =>
                {
                    long rowId = LsmIndexKeyCodec.RowIdOf(userKey);
                    if (!versions.TryGetValue(rowId, out (ulong Sequence, bool Live) current) || sequence > current.Sequence)
                    {
                        versions[rowId] = (sequence, type == LsmValueType.Put);
                    }
                });
            }
        }
        finally
        {
            version.Release();
        }

        if (_ambientWrite.Value?.StagedIndexFor(state) is { } staged)
        {
            foreach (KeyValuePair<byte[], bool> change in staged)
            {
                if (LsmIndexKeyCodec.PrefixOf(change.Key).SequenceEqual(prefix))
                {
                    newest[LsmIndexKeyCodec.RowIdOf(change.Key)] = (ulong.MaxValue, change.Value);
                }
            }
        }

        var rows = new List<long>(newest.Count);
        foreach (KeyValuePair<long, (ulong Sequence, bool Live)> entry in newest)
        {
            if (entry.Value.Live)
            {
                rows.Add(entry.Key);
            }
        }

        rows.Sort();
        return rows;
    }

    /// <summary>Every live (key, row id) entry of the index; used by the rare delete-by-row-id paths.</summary>
    internal List<(byte[] EncodedKey, long RowId)> IndexEntries(string databaseName, string tableName, string indexName)
    {
        IndexTableState state = GetOrCreateIndexTable(databaseName, tableName, indexName);
        var newest = new Dictionary<byte[], (ulong Sequence, bool Live)>(ByteArrayComparer.Instance);
        lock (state.OverlayGate)
        {
            foreach (KeyValuePair<byte[], Dictionary<long, (ulong Sequence, bool Live)>> key in state.Overlay)
            {
                foreach (KeyValuePair<long, (ulong Sequence, bool Live)> row in key.Value)
                {
                    newest[LsmIndexKeyCodec.UserKey(key.Key.AsSpan(LsmIndexKeyCodec.LengthPrefixSize), row.Key)] = row.Value;
                }
            }
        }

        TableVersion version = state.AcquireVersion();
        try
        {
            foreach (LsmSstableHandle file in version.Files)
            {
                file.GetKeyedReader(_readMode).ForEach(newest, static (userKey, sequence, type, _, versions) =>
                {
                    byte[] key = userKey.ToArray();
                    if (!versions.TryGetValue(key, out (ulong Sequence, bool Live) current) || sequence > current.Sequence)
                    {
                        versions[key] = (sequence, type == LsmValueType.Put);
                    }
                });
            }
        }
        finally
        {
            version.Release();
        }

        if (_ambientWrite.Value?.StagedIndexFor(state) is { } staged)
        {
            foreach (KeyValuePair<byte[], bool> change in staged)
            {
                newest[change.Key] = (ulong.MaxValue, change.Value);
            }
        }

        var entries = new List<(byte[] EncodedKey, long RowId)>();
        foreach (KeyValuePair<byte[], (ulong Sequence, bool Live)> entry in newest)
        {
            if (entry.Value.Live)
            {
                entries.Add((LsmIndexKeyCodec.PrefixOf(entry.Key)[LsmIndexKeyCodec.LengthPrefixSize..].ToArray(), LsmIndexKeyCodec.RowIdOf(entry.Key)));
            }
        }

        return entries;
    }

    /// <summary>
    /// Builds an index from scratch: replaces any previous index table with one sorted run holding
    /// <paramref name="entries"/> as of the last applied commit, then marks the index built. Used by
    /// CREATE INDEX on a populated table and to migrate indexes that predate persisted indexes.
    /// The caller holds the table's write lock, so no commit touches this index meanwhile.
    /// </summary>
    internal void BuildIndex(string databaseName, string tableName, string indexName, IEnumerable<(byte[] EncodedKey, long RowId)> entries)
    {
        DropIndexTable(databaseName, tableName, indexName);
        IndexTableState state = GetOrCreateIndexTable(databaseName, tableName, indexName);

        var userKeys = new List<byte[]>();
        foreach ((byte[] encodedKey, long rowId) in entries)
        {
            userKeys.Add(LsmIndexKeyCodec.UserKey(encodedKey, rowId));
        }

        userKeys.Sort(ByteArrayComparer.Instance);
        ulong sequence = checked((ulong)Math.Max(0, Volatile.Read(ref _appliedLsn)));
        if (userKeys.Count > 0)
        {
            long fileNumber = state.Manifest.AllocateFileNumber();
            string fileName = $"{fileNumber:D6}.sst";
            long length = 0;
            byte[]? first = null;
            byte[]? last = null;
            LsmDurableFileOperations.WriteFileAtomically(
                Path.Combine(state.TableDirectory, fileName),
                overwrite: false,
                stream =>
                {
                    using var writer = new SsTableKeyedWriter(stream, LsmIndexKeyCodec.RowIdSize);
                    byte[]? previous = null;
                    foreach (byte[] userKey in userKeys)
                    {
                        if (previous is not null && userKey.AsSpan().SequenceEqual(previous))
                        {
                            continue;
                        }

                        writer.Add(userKey, sequence, LsmValueType.Put, ReadOnlySpan<byte>.Empty);
                        previous = userKey;
                    }

                    length = writer.Finish();
                },
                crashHook: null,
                LsmCrashPoint.AfterSstableTempFileFsyncBeforeRename,
                LsmCrashPoint.AfterSstableRenameBeforeDirectoryFsync,
                LsmCrashPoint.AfterSstableDirectoryFsyncBeforeManifest);

            first = new byte[userKeys[0].Length + InternalKey.TagSize];
            InternalKey.Write(first, userKeys[0], sequence, LsmValueType.Put);
            last = new byte[userKeys[^1].Length + InternalKey.TagSize];
            InternalKey.Write(last, userKeys[^1], sequence, LsmValueType.Put);
            var metadata = new LsmTableFileMetadata(fileNumber, level: 0, first, last, length, fileName);
            var edit = new LsmVersionEdit();
            edit.AddFile(metadata);
            state.FileRegistry.ExecuteVersionEdit(() =>
            {
                state.Manifest.ApplyEdit(edit);
                state.FileRegistry.Register(metadata);
                return true;
            });
        }

        state.FlushedLsn = checked((long)sequence);

        // Durable last: an index directory without the marker is rebuilt on the next open.
        LsmDurableFileOperations.WriteFileAtomically(
            Path.Combine(state.TableDirectory, IndexBuiltMarker),
            overwrite: true,
            static _ => { },
            crashHook: null,
            LsmCrashPoint.AfterSstableTempFileFsyncBeforeRename,
            LsmCrashPoint.AfterSstableRenameBeforeDirectoryFsync,
            LsmCrashPoint.AfterSstableDirectoryFsyncBeforeManifest);
    }

    /// <summary>Closes and deletes an index table (DROP INDEX, or before a rebuild).</summary>
    internal void DropIndexTable(string databaseName, string tableName, string indexName)
    {
        if (_indexTables.TryRemove((databaseName, tableName, indexName), out IndexTableState? state))
        {
            state.Dispose();
        }

        string directory = GetIndexDirectory(databaseName, tableName, indexName);
        if (Directory.Exists(directory))
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private void StageIndexChange(string databaseName, string tableName, string indexName, byte[] encodedKey, long rowId, bool live)
    {
        IndexTableState state = GetOrCreateIndexTable(databaseName, tableName, indexName);
        PendingWrite pending = AcquireWrite(out bool ownsWrite);
        pending.StageIndex(state, LsmIndexKeyCodec.UserKey(encodedKey, rowId), live);
        if (live)
        {
            pending.Batch.IndexPut(databaseName, tableName, indexName, encodedKey, rowId);
        }
        else
        {
            pending.Batch.IndexDelete(databaseName, tableName, indexName, encodedKey, rowId);
        }

        CompleteWrite(pending, ownsWrite);
    }

    /// <summary>Applies an index operation of a committed frame to its index MemTable (commit gate held).</summary>
    private void ApplyIndexOperation(LsmCommitOperation operation, ulong sequence, bool replaying)
    {
        (string indexName, byte[] encodedKey) = LsmCommitBatch.DecodeIndexValue(operation.Value);
        IndexTableState state;
        if (replaying)
        {
            state = GetOrCreateIndexTable(operation.Database, operation.Table, indexName);
            if (checked((long)sequence) <= state.FlushedLsn)
            {
                return;
            }
        }
        else if (!_indexTables.TryGetValue((operation.Database, operation.Table, indexName), out state!))
        {
            return; // dropped while the statement was running
        }

        byte[] userKey = LsmIndexKeyCodec.UserKey(encodedKey, operation.RowId);
        bool live = operation.Kind == LsmCommitOperationKind.IndexPut;
        if (live)
        {
            state.Table.Put(userKey, sequence, ReadOnlySpan<byte>.Empty);
        }
        else
        {
            state.Table.Delete(userKey, sequence);
        }

        if (replaying)
        {
            lock (state.OverlayGate)
            {
                state.Publish(userKey, sequence, live);
            }
        }
    }

    /// <summary>Makes this flow's staged index changes visible to other sessions (publication turn held).</summary>
    private static void PublishStagedIndexes(PendingWrite pending, ulong sequence)
    {
        foreach (KeyValuePair<IndexTableState, Dictionary<byte[], bool>> index in pending.StagedIndexes)
        {
            lock (index.Key.OverlayGate)
            {
                foreach (KeyValuePair<byte[], bool> change in index.Value)
                {
                    index.Key.Publish(change.Key, sequence, change.Value);
                }
            }
        }
    }

    private IndexTableState GetOrCreateIndexTable(string databaseName, string tableName, string indexName)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfDisposed(_disposed, this);
        DataVo.Core.Compat.ThrowHelper.ThrowIfNullOrWhiteSpace(indexName);
        return _indexTables.GetOrAdd(
            (databaseName, tableName, indexName),
            static (key, owner) => owner.CreateIndexTableState(key.Database, key.Table, key.Index),
            this);
    }

    private IndexTableState CreateIndexTableState(string databaseName, string tableName, string indexName)
    {
        string directory = GetIndexDirectory(databaseName, tableName, indexName);
        Directory.CreateDirectory(directory);
        var manifest = new LsmManifest(Path.Combine(directory, "MANIFEST"));
        var table = new LsmTable(directory, manifest) { Format = LsmSstableFormat.Keyed, ReadMode = _readMode };
        if (MemTableFlushThresholdOverrideBytes is long thresholdOverride)
        {
            table.FlushThresholdBytes = thresholdOverride;
        }

        var registry = new LsmFileRegistry(directory, manifest);
        table.CompactionRegistry = registry;
        table.BeforeFlushDurabilityBarrier = maxSequence => _commitLog.EnsureDurableThrough(checked((long)maxSequence));
        table.AfterFlush = PruneCommitLog;
        var state = new IndexTableState(directory, manifest, table, registry);
        table.AfterSstablePublished = memTable => EvictFlushedIndexEntries(state, memTable);

        ulong flushed = 0;
        TableVersion version = state.AcquireVersion();
        try
        {
            foreach (LsmSstableHandle file in version.Files)
            {
                flushed = Math.Max(flushed, file.GetKeyedReader(_readMode).Properties.LargestSequence);
            }
        }
        finally
        {
            version.Release();
        }

        state.FlushedLsn = checked((long)flushed);
        return state;
    }

    private string GetIndexDirectory(string databaseName, string tableName, string indexName) =>
        Path.Combine(GetTableDirectory(databaseName, tableName), IndexDirectoryName, indexName);

    /// <summary>Opens the index tables found on disk for a table directory (LSN floor and replay).</summary>
    private long OpenExistingIndexTables(string databaseName, string tableName, string tableDirectory)
    {
        string indexRoot = Path.Combine(tableDirectory, IndexDirectoryName);
        if (!Directory.Exists(indexRoot))
        {
            return 0;
        }

        long floor = 0;
        foreach (string indexDirectory in Directory.EnumerateDirectories(indexRoot))
        {
            if (File.Exists(Path.Combine(indexDirectory, "MANIFEST")))
            {
                floor = Math.Max(floor, GetOrCreateIndexTable(databaseName, tableName, Path.GetFileName(indexDirectory)).FlushedLsn);
            }
        }

        return floor;
    }

    /// <summary>Disposes the open index tables of one table (or of a whole database when <paramref name="tableName"/> is null).</summary>
    private void CloseIndexTables(string databaseName, string? tableName)
    {
        foreach ((string Database, string Table, string Index) key in _indexTables.Keys)
        {
            if (string.Equals(key.Database, databaseName, StringComparison.Ordinal)
                && (tableName is null || string.Equals(key.Table, tableName, StringComparison.Ordinal))
                && _indexTables.TryRemove(key, out IndexTableState? state))
            {
                state.Dispose();
            }
        }
    }

    private static void EvictFlushedIndexEntries(IndexTableState state, MemTable memTable)
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
                state.Evict(InternalKey.UserKey(entry.InternalKey), flushedThrough);
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

    private sealed class IndexTableState(string directory, LsmManifest manifest, LsmTable table, LsmFileRegistry fileRegistry)
        : LsmKeySpaceState(directory, manifest, table, fileRegistry)
    {
        /// <summary>
        /// Published, unflushed entries: key prefix → row id → newest (sequence, live). Guarded by
        /// <see cref="LsmKeySpaceState.OverlayGate"/> for reads and writes.
        /// </summary>
        public Dictionary<byte[], Dictionary<long, (ulong Sequence, bool Live)>> Overlay { get; } = new(ByteArrayComparer.Instance);

        public void Publish(byte[] userKey, ulong sequence, bool live)
        {
            byte[] prefix = LsmIndexKeyCodec.PrefixOf(userKey).ToArray();
            if (!Overlay.TryGetValue(prefix, out Dictionary<long, (ulong Sequence, bool Live)>? rows))
            {
                rows = [];
                Overlay[prefix] = rows;
            }

            rows[LsmIndexKeyCodec.RowIdOf(userKey)] = (sequence, live);
        }

        public void Evict(ReadOnlySpan<byte> userKey, ulong flushedThrough)
        {
            byte[] prefix = LsmIndexKeyCodec.PrefixOf(userKey).ToArray();
            if (!Overlay.TryGetValue(prefix, out Dictionary<long, (ulong Sequence, bool Live)>? rows))
            {
                return;
            }

            long rowId = LsmIndexKeyCodec.RowIdOf(userKey);
            if (rows.TryGetValue(rowId, out (ulong Sequence, bool Live) version) && version.Sequence <= flushedThrough)
            {
                rows.Remove(rowId);
                if (rows.Count == 0)
                {
                    Overlay.Remove(prefix);
                }
            }
        }
    }

    /// <summary>Content equality and ordering for byte-array keys.</summary>
    private sealed class ByteArrayComparer : IEqualityComparer<byte[]>, IComparer<byte[]>
    {
        public static readonly ByteArrayComparer Instance = new();

        public bool Equals(byte[]? x, byte[]? y) =>
            ReferenceEquals(x, y) || (x is not null && y is not null && x.AsSpan().SequenceEqual(y));

        public int GetHashCode(byte[] key)
        {
            unchecked
            {
                int hash = (int)2166136261;
                foreach (byte b in key)
                {
                    hash = (hash ^ b) * 16777619;
                }

                return hash;
            }
        }

        public int Compare(byte[]? x, byte[]? y) => x.AsSpan().SequenceCompareTo(y);
    }
}
