using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Threading;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Catalog;
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

    private readonly string _storageDirectory;
    private readonly LsmWalDurabilityMode _walDurabilityMode;
    private readonly ConcurrentDictionary<(string DatabaseName, string TableName), TableState> _tables = new();
    private bool _disposed;

    /// <summary>Creates an LSM storage engine rooted at <paramref name="storageDirectory"/>.</summary>
    public LsmStorageEngine(
        string storageDirectory,
        LsmWalDurabilityMode walDurabilityMode = LsmWalDurabilityMode.StrictFsync)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(storageDirectory);
        _storageDirectory = storageDirectory;
        _walDurabilityMode = walDurabilityMode;
        Directory.CreateDirectory(_storageDirectory);
    }

    /// <summary>
    /// Inserts a row and takes ownership of <paramref name="rowBytes"/>: the buffer is retained as the
    /// latest in-memory row version (the MemTable copies it into its arena) and must not be mutated by
    /// the caller afterwards.
    /// </summary>
    public long InsertRow(string databaseName, string tableName, byte[] rowBytes)
    {
        ArgumentNullException.ThrowIfNull(rowBytes);
        TableState state = GetOrCreateTable(databaseName, tableName);

        long insertedRowId;
        LsmWalDurabilityTicket ticket;
        lock (state.SyncRoot)
        {
            long rowId = state.NextRowId++;
            ulong seqno = state.NextSeqno++;
            Span<byte> userKey = stackalloc byte[UserKeySize];
            EncodeRowId(rowId, userKey);
            ticket = state.Table.PutDeferDurability(userKey, seqno, rowBytes);
            ticket = state.Table.PutDeferDurability(
                NextRowIdMetadataKey,
                state.NextSeqno++,
                EncodeNextRowIdMetadata(state.NextRowId));
            state.LatestRows.Set(rowId, rowBytes);
            insertedRowId = rowId;
        }

        // Group commit: durability is awaited outside the table lock so concurrent strict-mode
        // writers share one fsync instead of serializing one fsync per operation.
        ticket.Wait();
        return insertedRowId;
    }

    /// <summary>
    /// Inserts rows and takes ownership of every buffer in <paramref name="rowsBytes"/> (see
    /// <see cref="InsertRow"/>). Row keys are encoded on the stack per entry — no per-row key or
    /// value copies.
    /// </summary>
    public List<long> InsertRows(string databaseName, string tableName, List<byte[]> rowsBytes)
    {
        ArgumentNullException.ThrowIfNull(rowsBytes);
        TableState state = GetOrCreateTable(databaseName, tableName);

        List<long> insertedRowIds;
        LsmWalDurabilityTicket ticket;
        lock (state.SyncRoot)
        {
            var rowIds = new List<long>(rowsBytes.Count);
            LsmBatchRowPutEntry[] batch = ArrayPool<LsmBatchRowPutEntry>.Shared.Rent(rowsBytes.Count);
            try
            {
                int count = 0;
                foreach (byte[] rowBytes in rowsBytes)
                {
                    ArgumentNullException.ThrowIfNull(rowBytes);
                    long rowId = state.NextRowId++;
                    ulong seqno = state.NextSeqno++;
                    batch[count++] = new LsmBatchRowPutEntry(rowId, seqno, rowBytes);
                    rowIds.Add(rowId);
                }

                ReadOnlySpan<LsmBatchRowPutEntry> entries = batch.AsSpan(0, count);
                ticket = state.Table.PutRowIdBatch(entries);
                ticket = state.Table.PutDeferDurability(
                    NextRowIdMetadataKey,
                    state.NextSeqno++,
                    EncodeNextRowIdMetadata(state.NextRowId));
                foreach (LsmBatchRowPutEntry entry in entries)
                {
                    state.LatestRows.Set(entry.RowId, entry.Value);
                }

                insertedRowIds = rowIds;
            }
            finally
            {
                // Entries hold row-buffer references; clear so the pool doesn't pin them.
                ArrayPool<LsmBatchRowPutEntry>.Shared.Return(batch, clearArray: true);
            }
        }

        ticket.Wait();
        return insertedRowIds;
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

        return state.LatestRows.TryGet(rowId, out byte[] rowBytes) switch
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

        lock (state.SyncRoot)
        {
            return CollectLiveRowsSorted(state);
        }
    }

    /// <inheritdoc />
    public void DeleteRow(string databaseName, string tableName, long rowId)
    {
        TableState state = GetOrCreateTable(databaseName, tableName);

        LsmWalDurabilityTicket ticket;
        lock (state.SyncRoot)
        {
            if (state.LatestRows.TryGet(rowId, out _) != LatestRowLookup.Live)
            {
                return;
            }

            ulong seqno = state.NextSeqno++;
            Span<byte> userKey = stackalloc byte[UserKeySize];
            EncodeRowId(rowId, userKey);
            ticket = state.Table.DeleteDeferDurability(userKey, seqno);
            state.LatestRows.SetTombstone(rowId);
        }

        ticket.Wait();
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
        LsmWalDurabilityTicket ticket;
        lock (state.SyncRoot)
        {
            if (state.LatestRows.TryGet(rowId, out byte[] latest) != LatestRowLookup.Live)
            {
                return false;
            }

            // Copy-on-write: published row buffers are immutable so lock-free readers can never
            // observe a half-applied patch; the new version becomes visible in one reference publish.
            byte[] rowBytes = latest.ToArray();
            for (int i = 0; i < ordinals.Length; i++)
            {
                if (!RowSerializer.TryOverwriteFixedWidthCell(rowBytes, columns, ordinals[i], values[i]))
                {
                    return false;
                }
            }

            ulong seqno = state.NextSeqno++;
            Span<byte> userKey = stackalloc byte[UserKeySize];
            EncodeRowId(rowId, userKey);
            ticket = state.Table.PutDeferDurability(userKey, seqno, rowBytes);
            state.LatestRows.Set(rowId, rowBytes);
        }

        ticket.Wait();
        return true;
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

        TableState state = GetOrCreateTable(databaseName, tableName);
        int patched;
        LsmWalDurabilityTicket pendingTicket;
        lock (state.SyncRoot)
        {
            if (ordinals.Length > 2)
            {
                throw new NotSupportedException("LSM fixed-width batch patch currently supports up to two assignments.");
            }

            LsmBatchRowPutEntry[] batch = ArrayPool<LsmBatchRowPutEntry>.Shared.Rent(operations.Count);
            try
            {
                int count = 0;
                foreach (FixedWidthPatchOperation operation in operations)
                {
                    if (state.LatestRows.TryGet(operation.RowId, out byte[] latest) != LatestRowLookup.Live)
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

                    ulong seqno = state.NextSeqno++;
                    batch[count++] = new LsmBatchRowPutEntry(operation.RowId, seqno, rowBytes);
                }

                ReadOnlySpan<LsmBatchRowPutEntry> entries = batch.AsSpan(0, count);
                LsmWalDurabilityTicket ticket = state.Table.PutRowIdBatch(entries);
                foreach (LsmBatchRowPutEntry entry in entries)
                {
                    state.LatestRows.Set(entry.RowId, entry.Value);
                }

                patched = count;
                pendingTicket = ticket;
            }
            finally
            {
                // Entries hold row-buffer references; clear so the pool doesn't pin them.
                ArrayPool<LsmBatchRowPutEntry>.Shared.Return(batch, clearArray: true);
            }
        }

        pendingTicket.Wait();
        return patched;
    }

    /// <inheritdoc />
    public void DropTable(string databaseName, string tableName)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
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
        ObjectDisposedException.ThrowIf(_disposed, this);
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
                state.Dispose();
            }
        }

        _tables.Clear();
    }

    private TableState GetOrCreateTable(string databaseName, string tableName)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        return _tables.GetOrAdd(
            (databaseName, tableName),
            static (key, owner) => owner.CreateTableState(key.DatabaseName, key.TableName),
            this);
    }

    /// <summary>Test hook: overrides the MemTable rotation threshold for tables created afterwards.</summary>
    internal long? MemTableFlushThresholdOverrideBytes { get; set; }

    private TableState CreateTableState(string databaseName, string tableName)
    {
        string tableDirectory = GetTableDirectory(databaseName, tableName);
        Directory.CreateDirectory(tableDirectory);

        var manifest = new LsmManifest(Path.Combine(tableDirectory, "MANIFEST"));
        var table = new LsmTable(
            tableDirectory,
            manifest,
            Path.Combine(tableDirectory, "active.wal"),
            _walDurabilityMode);
        if (MemTableFlushThresholdOverrideBytes is long thresholdOverride)
        {
            table.FlushThresholdBytes = thresholdOverride;
        }
        var registry = new LsmFileRegistry(tableDirectory, manifest);
        table.CompactionRegistry = registry;
        var state = new TableState(tableDirectory, manifest, table, registry);

        state.Table.FlushActiveMemTable();
        Dictionary<long, LatestRowVersion> latestRows = RebuildLatestRowsFromSstables(
            state,
            out long persistedNextRowId);
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

            if (version.Sequence >= state.NextSeqno)
            {
                state.NextSeqno = version.Sequence + 1;
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

        return state;
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
        out long persistedNextRowId)
    {
        var latest = new Dictionary<long, LatestRowVersion>();
        persistedNextRowId = 1;
        ulong persistedNextRowIdSeqno = 0;
        foreach (SstableEntry entry in ReadAllSstableEntries(state))
        {
            ulong seqno = InternalKey.Sequence(entry.InternalKey);
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

        public ulong NextSeqno { get; set; } = 1;

        public void Dispose()
        {
            Table.FlushActiveMemTable();
            Table.Dispose();
        }
    }

    private readonly record struct LatestRowVersion(byte[] Value, bool IsTombstone, ulong Sequence);

    private readonly record struct SstableEntry(byte[] InternalKey, byte[] Value);
}
