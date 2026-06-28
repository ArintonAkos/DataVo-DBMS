using System.Buffers.Binary;
using System.Collections.Concurrent;
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
    private const int MaxLevelToScan = 7;

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

    /// <inheritdoc />
    public long InsertRow(string databaseName, string tableName, byte[] rowBytes)
    {
        ArgumentNullException.ThrowIfNull(rowBytes);
        TableState state = GetOrCreateTable(databaseName, tableName);

        lock (state.SyncRoot)
        {
            long rowId = state.NextRowId++;
            ulong seqno = state.NextSeqno++;
            byte[] value = rowBytes.ToArray();
            Span<byte> userKey = stackalloc byte[UserKeySize];
            EncodeRowId(rowId, userKey);
            state.Table.Put(userKey, seqno, value);
            state.LatestRows[rowId] = new LatestRowVersion(value, IsTombstone: false, seqno);
            return rowId;
        }
    }

    /// <inheritdoc />
    public List<long> InsertRows(string databaseName, string tableName, List<byte[]> rowsBytes)
    {
        ArgumentNullException.ThrowIfNull(rowsBytes);
        TableState state = GetOrCreateTable(databaseName, tableName);

        lock (state.SyncRoot)
        {
            var rowIds = new List<long>(rowsBytes.Count);
            Span<byte> userKey = stackalloc byte[UserKeySize];
            foreach (byte[] rowBytes in rowsBytes)
            {
                ArgumentNullException.ThrowIfNull(rowBytes);
                long rowId = state.NextRowId++;
                ulong seqno = state.NextSeqno++;
                byte[] value = rowBytes.ToArray();
                EncodeRowId(rowId, userKey);
                state.Table.Put(userKey, seqno, value);
                state.LatestRows[rowId] = new LatestRowVersion(value, IsTombstone: false, seqno);
                rowIds.Add(rowId);
            }

            return rowIds;
        }
    }

    /// <inheritdoc />
    public byte[] ReadRow(string databaseName, string tableName, long rowId)
    {
        TableState state = GetOrCreateTable(databaseName, tableName);

        lock (state.SyncRoot)
        {
            if (!state.LatestRows.TryGetValue(rowId, out LatestRowVersion latest))
            {
                throw new RowNotFoundException(rowId, tableName);
            }

            if (latest.IsTombstone)
            {
                throw new RowDeletedException(rowId, tableName);
            }

            return latest.Value.ToArray();
        }
    }

    /// <inheritdoc />
    public IEnumerable<(long RowId, byte[] RawRow)> ReadAllRows(string databaseName, string tableName)
    {
        TableState state = GetOrCreateTable(databaseName, tableName);

        lock (state.SyncRoot)
        {
            return ReadLatestRows(state)
                .Where(pair => !pair.Value.IsTombstone)
                .OrderBy(pair => pair.Key)
                .Select(pair => (pair.Key, pair.Value.Value.ToArray()))
                .ToList();
        }
    }

    /// <inheritdoc />
    public void DeleteRow(string databaseName, string tableName, long rowId)
    {
        TableState state = GetOrCreateTable(databaseName, tableName);

        lock (state.SyncRoot)
        {
            if (!state.LatestRows.TryGetValue(rowId, out LatestRowVersion latest) || latest.IsTombstone)
            {
                return;
            }

            ulong seqno = state.NextSeqno++;
            Span<byte> userKey = stackalloc byte[UserKeySize];
            EncodeRowId(rowId, userKey);
            state.Table.Delete(userKey, seqno);
            state.LatestRows[rowId] = new LatestRowVersion([], IsTombstone: true, seqno);
        }
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
        lock (state.SyncRoot)
        {
            if (!state.LatestRows.TryGetValue(rowId, out LatestRowVersion latest) || latest.IsTombstone)
            {
                return false;
            }

            byte[] rowBytes = latest.Value;
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
            state.Table.Put(userKey, seqno, rowBytes);
            state.LatestRows[rowId] = new LatestRowVersion(rowBytes, IsTombstone: false, seqno);
            return true;
        }
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
            var compactor = new LsmCompactor(state.TableDirectory, state.Manifest, state.FileRegistry);
            compactor.CompactLevel(sourceLevel: 0, targetLevel: 1);

            return ReadLatestRows(state)
                .Where(pair => !pair.Value.IsTombstone)
                .OrderBy(pair => pair.Key)
                .Select(pair => (pair.Key, pair.Value.Value.ToArray()))
                .ToList();
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
        var registry = new LsmFileRegistry(tableDirectory, manifest);
        var state = new TableState(tableDirectory, manifest, table, registry);

        state.Table.FlushActiveMemTable();
        foreach ((long rowId, LatestRowVersion version) in RebuildLatestRowsFromSstables(state))
        {
            if (rowId >= state.NextRowId)
            {
                state.NextRowId = rowId + 1;
            }

            if (version.Sequence >= state.NextSeqno)
            {
                state.NextSeqno = version.Sequence + 1;
            }

            state.LatestRows[rowId] = version;
        }

        return state;
    }

    private string GetDatabaseDirectory(string databaseName) =>
        Path.Combine(_storageDirectory, databaseName);

    private string GetTableDirectory(string databaseName, string tableName) =>
        Path.Combine(GetDatabaseDirectory(databaseName), tableName);

    private static LatestRowVersion? TryReadLatestVersion(TableState state, long rowId)
    {
        state.Table.FlushActiveMemTable();
        return RebuildLatestRowsFromSstables(state).GetValueOrDefault(rowId);
    }

    private static Dictionary<long, LatestRowVersion> ReadLatestRows(TableState state)
    {
        state.Table.FlushActiveMemTable();
        state.LatestRows.Clear();
        foreach ((long rowId, LatestRowVersion version) in RebuildLatestRowsFromSstables(state))
        {
            state.LatestRows[rowId] = version;
        }

        return new Dictionary<long, LatestRowVersion>(state.LatestRows);
    }

    private static Dictionary<long, LatestRowVersion> RebuildLatestRowsFromSstables(TableState state)
    {
        var latest = new Dictionary<long, LatestRowVersion>();
        foreach (SstableEntry entry in ReadAllSstableEntries(state))
        {
            long rowId = DecodeRowId(InternalKey.UserKey(entry.InternalKey));
            ulong seqno = InternalKey.Sequence(entry.InternalKey);
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

    private static byte[] EncodeRowId(long rowId)
    {
        var userKey = new byte[UserKeySize];
        EncodeRowId(rowId, userKey);
        return userKey;
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

    private sealed class TableState : IDisposable
    {
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

        public Dictionary<long, LatestRowVersion> LatestRows { get; } = [];

        public long NextRowId { get; set; } = 1;

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
