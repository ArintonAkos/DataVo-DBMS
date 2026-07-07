using System.Collections.Concurrent;
using DataVo.Core.BTree;
using DataVo.Core.BTree.Core;
using DataVo.Core.Exceptions;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.Utils;
using DataVo.Core.Indexing.BTree;
using DataVo.Core.Indexing.Flat;
using DataVo.Core.Indexing.HNSW;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.Runtime.Diagnostics;

// Identifies a cached index by (database, table, index) without allocating a per-call string key.
// Case-insensitivity (previously provided by ToLowerInvariant + StringComparer.OrdinalIgnoreCase) is
// preserved by IndexManager.CacheKeyComparer.
using IndexCacheKey = (string Database, string Table, string Index);

namespace DataVo.Core.Indexing;

/// <summary>
/// Specifies how index mutations are persisted to disk.
/// </summary>
public enum IndexPersistenceMode
{
    /// <summary>
    /// Persist each mutation immediately after it is applied.
    /// </summary>
    Immediate,

    /// <summary>
    /// Buffer mutations in memory and flush them after a configured threshold is reached.
    /// </summary>
    Buffered,

    /// <summary>
    /// Never persist index mutations — the index lives purely in memory. Used by in-memory storage mode,
    /// where serializing the index to disk on every write is both incorrect (no disk) and O(n) per write.
    /// </summary>
    None,
}

/// <summary>
/// Central coordinator for all index instances, supporting multiple index types via factory pattern.
/// </summary>
/// <remarks>
/// <para>
/// This refactored IndexManager:
/// - Maintains a plugin registry of index factories (BTree, HNSW, B25, etc.)
/// - Routes index creation/loading to the appropriate factory
/// - Abstracts persistence through IIndexPersistence implementations
/// - Manages in-memory cache of loaded indices with unified metadata
/// - Handles lifecycle management (creation, mutation tracking, flushing)
/// </para>
/// <para>
/// The factory pattern allows new index types to be registered without modifying
/// IndexManager itself, following the Open/Closed principle.
/// </para>
/// </remarks>
public class IndexManager : IDisposable
{
    private static readonly HashSet<string> KnownVectorTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        "HNSW",
        "FLAT"
    };

    private static IndexManager? _instance;
    private readonly string _indexRootDirectory;

    /// <summary>
    /// Factory registry: maps index type name (e.g., "HNSW") to its factory.
    /// </summary>
    private readonly Dictionary<string, IIndexFactory> _factories = [];

    /// <summary>
    /// Persistence registry: maps index type to its persistence handler.
    /// </summary>
    private readonly Dictionary<string, IIndexPersistence> _persistenceHandlers = [];

    /// <summary>
    /// Unified index cache: maps CacheKey -> loaded index instance.
    /// </summary>
    private readonly Dictionary<IndexCacheKey, IIndexBase> _cache = new(CacheKeyComparer.Instance);

    /// <summary>
    /// Metadata storage: maps CacheKey -> index metadata.
    /// </summary>
    private readonly Dictionary<IndexCacheKey, IndexMetadata> _metadata = new(CacheKeyComparer.Instance);

    /// <summary>
    /// Tracks paths for cache entries: maps CacheKey -> file path.
    /// </summary>
    private readonly Dictionary<IndexCacheKey, string> _cachePaths = new(CacheKeyComparer.Instance);

    /// <summary>
    /// Tracks dirty (modified) indices that need flushing.
    /// </summary>
    private readonly HashSet<IndexCacheKey> _dirtyIndices = new(CacheKeyComparer.Instance);

    /// <summary>
    /// Tracks mutation count per index for buffered persistence.
    /// </summary>
    private readonly Dictionary<IndexCacheKey, int> _pendingMutations = new(CacheKeyComparer.Instance);

    /// <summary>
    /// Fast lane for integer primary keys: logical integer key -> physical row id.
    /// This is intentionally in-memory only and mirrors the authoritative scalar index for hot point lookups.
    /// Concurrent on both levels so point lookups (the hot read path) never serialize on <see cref="_lock"/>;
    /// upserts/removals during inserts and updates are individually atomic.
    /// </summary>
    private readonly ConcurrentDictionary<IndexCacheKey, ConcurrentInt64Int64Map> _integerPrimaryKeyMaps = new(CacheKeyComparer.Instance);

    /// <summary>
    /// Fast lane for single-column integer indexes: logical integer key -> physical row ids.
    /// Mirrors scalar BTree indexes for hot in-memory equality predicates.
    /// </summary>
    private readonly Dictionary<IndexCacheKey, Dictionary<long, List<long>>> _integerIndexMaps = new(CacheKeyComparer.Instance);

    /// <summary>
    /// Fast lane for GUID primary keys: logical GUID key -> physical row id.
    /// </summary>
    private readonly ConcurrentDictionary<IndexCacheKey, ConcurrentDictionary<Guid, long>> _guidPrimaryKeyMaps = new(CacheKeyComparer.Instance);

    /// <summary>
    /// Fast lane for single-column GUID indexes: logical GUID key -> physical row ids.
    /// </summary>
    private readonly Dictionary<IndexCacheKey, Dictionary<Guid, List<long>>> _guidIndexMaps = new(CacheKeyComparer.Instance);

    private readonly object _lock = new();
    private IndexPersistenceMode _persistenceMode = IndexPersistenceMode.Immediate;
    private int _flushMutationThreshold = 256;

    /// <summary>
    /// Initializes a new polymorphic index manager with default configuration.
    /// </summary>
    public IndexManager()
        : this(config: null, engineStorageRoot: null)
    {
    }

    /// <summary>
    /// Initializes a new polymorphic index manager with optional configuration.
    /// </summary>
    public IndexManager(DataVoConfig? config, string? engineStorageRoot)
    {
        _indexRootDirectory = ResolveIndexRootDirectory(config, engineStorageRoot);
        Directory.CreateDirectory(_indexRootDirectory);
        EnsureDefaultRegistrations();

        // In-memory contexts never persist indexes: serializing the whole index to disk on every write is
        // both wrong (no disk) and O(n) per write (O(n^2) bulk load). Disk mode keeps immediate persistence.
        if (config?.StorageMode == StorageMode.InMemory)
        {
            _persistenceMode = IndexPersistenceMode.None;
        }
    }

    /// <summary>
    /// Persists the index now only in <see cref="IndexPersistenceMode.Immediate"/> mode. In
    /// <see cref="IndexPersistenceMode.Buffered"/> mode the threshold flush handles it; in
    /// <see cref="IndexPersistenceMode.None"/> mode the index is never serialized.
    /// </summary>
    private void FlushIfImmediate(IndexCacheKey cacheKey)
    {
        if (_persistenceMode == IndexPersistenceMode.Immediate)
        {
            FlushInternal(cacheKey);
        }
    }

    /// <summary>
    /// Gets the singleton instance (for backward compatibility).
    /// </summary>
    public static IndexManager Instance
    {
        get
        {
            _instance ??= new IndexManager();
            return _instance;
        }
    }

    /// <summary>
    /// Registers an index type's factory and persistence handler.
    /// </summary>
    /// <param name="indexType">Type identifier (e.g., "BTREE", "HNSW").</param>
    /// <param name="factory">Factory for creating indices of this type.</param>
    /// <param name="persistence">Persistence handler for this type.</param>
    public void RegisterIndexType(string indexType, IIndexFactory factory, IIndexPersistence persistence)
    {
        lock (_lock)
        {
            _factories[indexType.ToUpper()] = factory;
            _persistenceHandlers[indexType.ToUpper()] = persistence;
        }
    }

    /// <summary>
    /// Returns whether the provided index type is registered and supports vector search capabilities.
    /// </summary>
    public bool SupportsVectorIndexType(string indexType)
    {
        if (string.IsNullOrWhiteSpace(indexType))
        {
            return false;
        }

        lock (_lock)
        {
            string normalizedType = indexType.ToUpperInvariant();
            if (!_factories.TryGetValue(normalizedType, out var factory))
            {
                return false;
            }

            return factory is IVectorIndexFactory
                || KnownVectorTypes.Contains(normalizedType)
                || string.Equals(factory.IndexType, normalizedType, StringComparison.OrdinalIgnoreCase)
                    && KnownVectorTypes.Contains(factory.IndexType);
        }
    }

    /// <summary>
    /// Creates a new index of the specified type.
    /// </summary>
    public object CreateIndex(string indexType, IndexMetadata metadata, Dictionary<string, object> @params)
    {
        lock (_lock)
        {
            var type = indexType.ToUpper();
            if (!_factories.TryGetValue(type, out var factory))
                throw new NotSupportedException($"Index type '{indexType}' not registered");

            object rawIndex = factory.CreateIndex(metadata.IndexName, metadata.ColumnName, @params);
            IIndexBase index = EnsureIndexBase(rawIndex, metadata.IndexType, CacheKeyOf(metadata));

            _cache[CacheKeyOf(metadata)] = index;
            _metadata[CacheKeyOf(metadata)] = metadata;
            _dirtyIndices.Add(CacheKeyOf(metadata));

            return index;
        }
    }

    /// <summary>
    /// Loads an index from disk or memory cache.
    /// </summary>
    public object? TryLoadIndex(IndexMetadata metadata, string? overridePath = null)
    {
        lock (_lock)
        {
            // Check if already loaded
            if (_cache.TryGetValue(CacheKeyOf(metadata), out IIndexBase? cached))
                return cached;

            var type = metadata.IndexType.ToUpper();
            if (!_persistenceHandlers.TryGetValue(type, out var persistence))
                throw new NotSupportedException($"Index type '{metadata.IndexType}' has no persistence handler");

            var filePath = overridePath ?? BuildIndexPath(metadata);
            if (!persistence.FileExists(filePath))
                return null;

            object rawIndex = persistence.LoadIndex(filePath);
            IIndexBase index = EnsureIndexBase(rawIndex, metadata.IndexType, CacheKeyOf(metadata));
            _cache[CacheKeyOf(metadata)] = index;
            _metadata[CacheKeyOf(metadata)] = metadata;
            _cachePaths[CacheKeyOf(metadata)] = filePath;

            return index;
        }
    }

    /// <summary>
    /// Marks an index as dirty (requiring flush to disk).
    /// </summary>
    public void MarkDirty(IndexCacheKey cacheKey)
    {
        lock (_lock)
            MarkDirtyNoLock(cacheKey);

        TrackMutation(cacheKey);
    }

    private void MarkDirtyNoLock(IndexCacheKey cacheKey)
    {
        _dirtyIndices.Add(cacheKey);
    }

    /// <summary>
    /// Tracks a mutation and flushes if threshold exceeded (buffered mode).
    /// </summary>
    private void TrackMutation(IndexCacheKey cacheKey)
    {
        if (_persistenceMode != IndexPersistenceMode.Buffered)
            return;

        lock (_lock)
        {
            _pendingMutations.TryGetValue(cacheKey, out var count);
            _pendingMutations[cacheKey] = count + 1;

            if (_pendingMutations[cacheKey] >= _flushMutationThreshold)
            {
                FlushInternal(cacheKey);
                _pendingMutations[cacheKey] = 0;
            }
        }
    }

    private void TrackMutations(IndexCacheKey cacheKey, int count)
    {
        if (_persistenceMode != IndexPersistenceMode.Buffered || count <= 0)
            return;

        lock (_lock)
        {
            _pendingMutations.TryGetValue(cacheKey, out var current);
            int next = current + count;
            _pendingMutations[cacheKey] = next;

            if (next >= _flushMutationThreshold)
            {
                FlushInternal(cacheKey);
                _pendingMutations[cacheKey] = 0;
            }
        }
    }

    /// <summary>
    /// Flushes all dirty indices to disk.
    /// </summary>
    public void FlushAll()
    {
        lock (_lock)
        {
            foreach (var cacheKey in _dirtyIndices.ToList())
                FlushInternal(cacheKey);
        }
    }

    private void FlushInternal(IndexCacheKey cacheKey)
    {
        // In-memory contexts never serialize indexes. Guard here (not just at the call sites) so no flush
        // path — explicit FlushAll, dirty-on-read flushes, disposal — ever serializes an in-memory index.
        if (_persistenceMode == IndexPersistenceMode.None)
        {
            _dirtyIndices.Remove(cacheKey);
            return;
        }

        if (!_cache.TryGetValue(cacheKey, out IIndexBase? index))
            return;
        if (!_metadata.TryGetValue(cacheKey, out var metadata))
            return;

        var type = metadata.IndexType.ToUpper();
        if (!_persistenceHandlers.TryGetValue(type, out var persistence))
            throw new InvalidOperationException($"No persistence handler registered for index type '{metadata.IndexType}' ({cacheKey}).");

        var filePath = _cachePaths.TryGetValue(cacheKey, out var path)
            ? path
            : BuildIndexPath(metadata);

        persistence.SaveIndex(index, filePath);
        persistence.Flush(index);

        _cachePaths[cacheKey] = filePath;
        _dirtyIndices.Remove(cacheKey);
        metadata.ModifiedAtUtc = DateTime.UtcNow;
    }

    /// <summary>
    /// Gets an index from cache (no disk loading).
    /// </summary>
    public IIndexBase? TryGetCached(IndexCacheKey cacheKey)
    {
        lock (_lock)
            return _cache.TryGetValue(cacheKey, out var index) ? index : null;
    }

    /// <summary>
    /// Gets metadata for an index.
    /// </summary>
    public IndexMetadata? TryGetMetadata(IndexCacheKey cacheKey)
    {
        lock (_lock)
            return _metadata.TryGetValue(cacheKey, out var meta) ? meta : null;
    }

    /// <summary>
    /// Removes an index from cache and optionally deletes its disk file.
    /// </summary>
    public void RemoveIndex(IndexCacheKey cacheKey, bool deleteFile = false)
    {
        lock (_lock)
        {
            _metadata.TryGetValue(cacheKey, out var metadata);

            _cache.Remove(cacheKey);
            _metadata.Remove(cacheKey);
            _dirtyIndices.Remove(cacheKey);
            _pendingMutations.Remove(cacheKey);
            _integerPrimaryKeyMaps.TryRemove(cacheKey, out _);
            _guidPrimaryKeyMaps.TryRemove(cacheKey, out _);
            _integerIndexMaps.Remove(cacheKey);
            _guidIndexMaps.Remove(cacheKey);

            if (deleteFile && _cachePaths.TryGetValue(cacheKey, out var path))
            {
                string? indexType = metadata?.IndexType;
                if (!string.IsNullOrWhiteSpace(indexType)
                    && _persistenceHandlers.TryGetValue(indexType.ToUpperInvariant(), out var persistence))
                {
                    if (!persistence.TryDeleteFile(path) && File.Exists(path))
                    {
                        throw new InvalidOperationException($"Failed to delete index file '{path}'.");
                    }
                }
                else
                {
                    File.Delete(path);
                }
            }

            _cachePaths.Remove(cacheKey);
        }
    }

    /// <summary>
    /// Sets persistence mode and mutation threshold.
    /// </summary>
    public void SetPersistenceMode(IndexPersistenceMode mode, int flushThreshold = 256)
    {
        if (flushThreshold <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(flushThreshold), "Flush threshold must be greater than zero.");
        }

        lock (_lock)
        {
            _persistenceMode = mode;
            _flushMutationThreshold = flushThreshold;
        }
    }

    /// <summary>
    /// Creates or replaces a scalar BTree index.
    /// </summary>
    public void CreateIndex(Dictionary<string, List<long>> values, string indexName, string tableName, string databaseName, IndexType? indexType = null)
    {
        IndexMetadata metadata = CreateScalarMetadata(indexName, tableName, databaseName);

        var index = (IIndex)CreateIndex("BTREE", metadata, []);

        foreach (var entry in values)
        {
            foreach (long rowId in entry.Value)
            {
                index.Insert(entry.Key, rowId);
            }
        }

        _cachePaths[CacheKeyOf(metadata)] = BuildIndexPath(metadata);
        MarkDirty(CacheKeyOf(metadata));
        FlushIfImmediate(CacheKeyOf(metadata));
    }

    /// <summary>
    /// Rebuilds an existing scalar BTree index from supplied values.
    /// </summary>
    public void RebuildIndex(Dictionary<string, List<long>> values, string indexName, string tableName, string databaseName, IndexType? indexType = null)
    {
        DropIndex(indexName, tableName, databaseName);
        CreateIndex(values, indexName, tableName, databaseName, indexType);
    }

    /// <summary>
    /// Drops a single index (scalar or vector).
    /// </summary>
    public void DropIndex(string indexName, string tableName, string databaseName)
    {
        var cacheKey = GetCacheKey(indexName, tableName, databaseName);
        RemoveIndex(cacheKey);

        string[] tableDirectories =
        [
            Path.Combine(_indexRootDirectory, databaseName, tableName),
            Path.Combine(_indexRootDirectory, databaseName.ToLowerInvariant(), tableName.ToLowerInvariant())
        ];

        foreach (string tableDirectory in tableDirectories.Distinct(StringComparer.OrdinalIgnoreCase))
        {
            if (!Directory.Exists(tableDirectory))
            {
                continue;
            }

            foreach (var persistence in _persistenceHandlers.Values.Distinct())
            {
                string[] candidatePaths =
                [
                    Path.Combine(tableDirectory, $"{indexName}{persistence.FileExtension}"),
                    Path.Combine(tableDirectory, $"{indexName.ToLowerInvariant()}{persistence.FileExtension}")
                ];

                foreach (string filePath in candidatePaths.Distinct(StringComparer.OrdinalIgnoreCase))
                {
                    if (!persistence.TryDeleteFile(filePath) && File.Exists(filePath))
                    {
                        throw new IOException($"Failed to delete index file '{filePath}'.");
                    }
                }
            }
        }

        // Delete legacy-formatted scalar index files if they exist.
        string[] legacyScalarPaths =
        [
            Path.Combine(_indexRootDirectory, databaseName, $"{tableName}_{indexName}_index.btree"),
            Path.Combine(_indexRootDirectory, databaseName.ToLowerInvariant(), $"{tableName.ToLowerInvariant()}_{indexName.ToLowerInvariant()}_index.btree")
        ];

        foreach (string legacyScalarPath in legacyScalarPaths.Distinct(StringComparer.OrdinalIgnoreCase))
        {
            if (!File.Exists(legacyScalarPath))
            {
                continue;
            }

            try
            {
                File.Delete(legacyScalarPath);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to delete legacy index file '{legacyScalarPath}'.", ex);
            }
        }
    }

    /// <summary>
    /// Drops all indexes for a database.
    /// </summary>
    public void DropDatabaseIndexes(string databaseName)
    {
        string databaseDirectory = Path.Combine(_indexRootDirectory, databaseName);
        if (Directory.Exists(databaseDirectory))
        {
            bool deleted = false;
            foreach (var persistence in _persistenceHandlers.Values.Distinct())
            {
                if (persistence.TryDeleteDirectory(databaseDirectory))
                {
                    deleted = true;
                    break;
                }
            }

            if (!deleted)
            {
                Directory.Delete(databaseDirectory, recursive: true);
            }
        }

        List<IndexCacheKey> keysToRemove;
        lock (_lock)
        {
            keysToRemove = _cache.Keys
                .Where(cacheKey => string.Equals(cacheKey.Database, databaseName, StringComparison.OrdinalIgnoreCase))
                .ToList();

            foreach (IndexCacheKey cacheKey in _integerPrimaryKeyMaps.Keys.Where(cacheKey => string.Equals(cacheKey.Database, databaseName, StringComparison.OrdinalIgnoreCase)).ToList())
            {
                _integerPrimaryKeyMaps.TryRemove(cacheKey, out _);
            }

            foreach (IndexCacheKey cacheKey in _guidPrimaryKeyMaps.Keys.Where(cacheKey => string.Equals(cacheKey.Database, databaseName, StringComparison.OrdinalIgnoreCase)).ToList())
            {
                _guidPrimaryKeyMaps.TryRemove(cacheKey, out _);
            }

            foreach (IndexCacheKey cacheKey in _integerIndexMaps.Keys.Where(cacheKey => string.Equals(cacheKey.Database, databaseName, StringComparison.OrdinalIgnoreCase)).ToList())
            {
                _integerIndexMaps.Remove(cacheKey);
            }

            foreach (IndexCacheKey cacheKey in _guidIndexMaps.Keys.Where(cacheKey => string.Equals(cacheKey.Database, databaseName, StringComparison.OrdinalIgnoreCase)).ToList())
            {
                _guidIndexMaps.Remove(cacheKey);
            }
        }

        foreach (IndexCacheKey cacheKey in keysToRemove)
        {
            RemoveIndex(cacheKey);
        }
    }

    /// <summary>
    /// Clears all loaded index state for this engine and removes persisted index files.
    /// </summary>
    public void ClearRuntimeStateAndDeleteAllIndexes()
    {
        lock (_lock)
        {
            _cache.Clear();
            _metadata.Clear();
            _cachePaths.Clear();
            _dirtyIndices.Clear();
            _pendingMutations.Clear();
            _integerPrimaryKeyMaps.Clear();
            _integerIndexMaps.Clear();
            _guidPrimaryKeyMaps.Clear();
            _guidIndexMaps.Clear();
        }

        if (!Directory.Exists(_indexRootDirectory))
        {
            Directory.CreateDirectory(_indexRootDirectory);
            return;
        }

        foreach (string directory in Directory.GetDirectories(_indexRootDirectory))
        {
            Directory.Delete(directory, recursive: true);
        }

        foreach (string filePath in Directory.GetFiles(_indexRootDirectory))
        {
            File.Delete(filePath);
        }

        Directory.CreateDirectory(_indexRootDirectory);
    }

    /// <summary>
    /// Returns whether a scalar index can be loaded and queried.
    /// </summary>
    public bool IsIndexHealthy(string indexName, string tableName, string databaseName)
    {
        try
        {
            _ = GetOrLoadScalarIndex(indexName, tableName, databaseName);
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Attempts to recover a scalar index by rebuilding it from source data.
    /// </summary>
    public bool TryRecoverIndex(
        string indexName,
        string tableName,
        string databaseName,
        Dictionary<string, List<long>> rebuildData,
        IndexType? indexType = null)
    {
        if (IsIndexHealthy(indexName, tableName, databaseName))
        {
            return true;
        }

        try
        {
            RebuildIndex(rebuildData, indexName, tableName, databaseName, indexType);
            return IsIndexHealthy(indexName, tableName, databaseName);
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Builds the standard index file path.
    /// </summary>
    private string BuildIndexPath(IndexMetadata metadata)
    {
        var type = metadata.IndexType.ToUpper();
        if (!_persistenceHandlers.TryGetValue(type, out var persistence))
            throw new NotSupportedException($"Unknown index type: {type}");

        string normalizedDatabase = metadata.DatabaseName.ToLowerInvariant();
        string normalizedTable = metadata.TableName.ToLowerInvariant();
        string normalizedIndexName = metadata.IndexName.ToLowerInvariant();

        var dir = Path.Combine(_indexRootDirectory, normalizedDatabase, normalizedTable);
        persistence.EnsureDirectory(dir);

        var filename = $"{normalizedIndexName}{persistence.FileExtension}";
        return Path.Combine(dir, filename);
    }

    private static string ResolveIndexRootDirectory(DataVoConfig? config, string? engineStorageRoot)
    {
        if (!string.IsNullOrEmpty(engineStorageRoot))
            return engineStorageRoot;

        if (config == null)
            return "databases";

        if (config.StorageMode == StorageMode.Disk)
            return config.DiskStoragePath ?? "./datavo_data";

        return Path.Combine(Path.GetTempPath(), "datavo_indexes", Guid.NewGuid().ToString("N"));
    }

    /// <summary>
    /// Flushes managed indexes and releases delegated manager resources.
    /// </summary>
    public void Dispose()
    {
        FlushAll();
    }

    private void EnsureDefaultRegistrations()
    {
        RegisterIndexType("BTREE", new BTreeIndexFactory(), new BTreeIndexPersistence());
        RegisterIndexType("HNSW", new HNSWIndexFactory(), new HNSWIndexPersistence());
        RegisterIndexType("FLAT", new FlatVectorIndexFactory(), new FlatVectorIndexPersistence());
    }

    private static IndexCacheKey GetCacheKey(string indexName, string tableName, string databaseName)
    {
        return (databaseName, tableName, indexName);
    }

    private static IndexCacheKey CacheKeyOf(IndexMetadata metadata)
    {
        return (metadata.DatabaseName, metadata.TableName, metadata.IndexName);
    }

    // Case-insensitive comparer for IndexCacheKey, preserving the previous OrdinalIgnoreCase semantics
    // of the string cache key (database/table/index names match case-insensitively).
    private sealed class CacheKeyComparer : IEqualityComparer<IndexCacheKey>
    {
        public static readonly CacheKeyComparer Instance = new();

        public bool Equals(IndexCacheKey x, IndexCacheKey y)
        {
            return string.Equals(x.Database, y.Database, StringComparison.OrdinalIgnoreCase)
                && string.Equals(x.Table, y.Table, StringComparison.OrdinalIgnoreCase)
                && string.Equals(x.Index, y.Index, StringComparison.OrdinalIgnoreCase);
        }

        public int GetHashCode(IndexCacheKey key)
        {
            return HashCode.Combine(
                StringComparer.OrdinalIgnoreCase.GetHashCode(key.Database),
                StringComparer.OrdinalIgnoreCase.GetHashCode(key.Table),
                StringComparer.OrdinalIgnoreCase.GetHashCode(key.Index));
        }
    }

    /// <summary>
    /// Compatibility API: creates or replaces a vector index.
    /// </summary>
    public void CreateVectorIndex(
        IEnumerable<(long RowId, float[] Vector)> vectors,
        string indexName,
        string tableName,
        string databaseName,
        string metric = "cosine",
        string indexType = "HNSW")
    {
        var cacheKey = GetCacheKey(indexName, tableName, databaseName);
        var metadata = CreateVectorMetadata(indexName, tableName, databaseName, indexType, metric);

        if (!SupportsVectorIndexType(metadata.IndexType))
        {
            throw new NotSupportedException($"Index type '{metadata.IndexType}' is not registered as a vector index type.");
        }

        var vectorIndex = (IVectorIndex)CreateIndex(metadata.IndexType, metadata, metadata.Parameters);
        if (vectorIndex is HNSWIndex hnsw)
        {
            hnsw.Metric = metric.Equals("l2", StringComparison.OrdinalIgnoreCase) || metric.Equals("euclidean", StringComparison.OrdinalIgnoreCase)
                ? "euclidean"
                : "cosine";
        }

        vectorIndex.Clear();

        foreach (var (rowId, vector) in vectors)
        {
            // No defensive copy: vector index implementations copy into their own backing store
            // on insert, so [.. vector] here was a redundant per-insert float[] allocation.
            vectorIndex.Insert(rowId, vector);
        }

        _cachePaths[cacheKey] = BuildIndexPath(metadata);
        MarkDirty(cacheKey);
        FlushIfImmediate(cacheKey);
    }

    /// <summary>
    /// Inserts or updates a single vector in an existing vector index.
    /// </summary>
    public void InsertIntoVectorIndex(float[] vector, long rowId, string indexName, string tableName, string databaseName, string indexType = "HNSW")
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(vector);
        InsertIntoVectorIndex(vector.AsSpan(), rowId, indexName, tableName, databaseName, indexType);
    }

    internal void InsertIntoVectorIndex(ReadOnlySpan<float> vector, long rowId, string indexName, string tableName, string databaseName, string indexType = "HNSW")
    {
        var cacheKey = GetCacheKey(indexName, tableName, databaseName);
        var vectorIndex = GetOrLoadVectorIndex(indexName, tableName, databaseName, indexType);
        if (vectorIndex is ISpanVectorIndex spanVectorIndex)
        {
            spanVectorIndex.Insert(rowId, vector);
        }
        else
        {
            vectorIndex.Insert(rowId, vector.ToArray());
        }

        MarkDirty(cacheKey);
        FlushIfImmediate(cacheKey);
    }

    /// <summary>
    /// Deletes vectors by row IDs.
    /// </summary>
    public void DeleteFromVectorIndex(List<long> toBeDeletedIds, string indexName, string tableName, string databaseName, string indexType = "HNSW")
    {
        var cacheKey = GetCacheKey(indexName, tableName, databaseName);
        var vectorIndex = GetOrLoadVectorIndex(indexName, tableName, databaseName, indexType);
        vectorIndex.Delete(toBeDeletedIds);
        MarkDirty(cacheKey);
        FlushIfImmediate(cacheKey);
    }

    /// <summary>
    /// Pre-allocates storage for vector index implementations that support reservation.
    /// </summary>
    public void ReserveVectorIndex(string indexName, string tableName, string databaseName, string indexType, int expectedCount, int vectorDimension)
    {
        if (expectedCount <= 0)
        {
            return;
        }

        var vectorIndex = GetOrLoadVectorIndex(indexName, tableName, databaseName, indexType);
        if (vectorIndex is IReservableVectorIndex reservable)
        {
            reservable.Reserve(expectedCount, vectorDimension);
        }
    }

    /// <summary>
    /// Performs nearest-neighbor vector search.
    /// </summary>
    public List<long> SearchVector(float[] queryVector, int topK, string indexName, string tableName, string databaseName, string indexType = "HNSW")
    {
        if (topK <= 0)
        {
            return [];
        }

        var vectorIndex = GetOrLoadVectorIndex(indexName, tableName, databaseName, indexType);
        List<long> results = vectorIndex.SearchTopK(queryVector, topK);
        RuntimeQueryDiagnosticsScope.RecordVectorSearch(indexName, topK, expansionPasses: 0);
        return results;
    }

    /// <summary>
    /// Inserts a scalar key into a BTree index.
    /// </summary>
    public void InsertIntoIndex(string value, long rowId, string indexName, string tableName, string databaseName)
    {
        lock (_lock)
        {
            var cacheKey = GetCacheKey(indexName, tableName, databaseName);
            IIndex index = GetOrLoadScalarIndex(indexName, tableName, databaseName);
            index.Insert(value, rowId);
            MarkDirtyNoLock(cacheKey);
            FlushIfImmediate(cacheKey);
        }

        TrackMutation(GetCacheKey(indexName, tableName, databaseName));
    }

    /// <summary>
    /// Inserts a batch of scalar keys into one BTree index under one manager lock.
    /// </summary>
    public void InsertManyIntoIndex(
        IReadOnlyList<(string Value, long RowId)> entries,
        string indexName,
        string tableName,
        string databaseName)
    {
        if (entries.Count == 0)
        {
            return;
        }

        var cacheKey = GetCacheKey(indexName, tableName, databaseName);
        lock (_lock)
        {
            IIndex index = GetOrLoadScalarIndex(indexName, tableName, databaseName);
            for (int i = 0; i < entries.Count; i++)
            {
                (string value, long rowId) = entries[i];
                index.Insert(value, rowId);
            }

            MarkDirtyNoLock(cacheKey);
            FlushIfImmediate(cacheKey);
        }

        TrackMutations(cacheKey, entries.Count);
    }

    /// <summary>
    /// Inserts integer primary-key entries into the direct key -> row-id fast lane.
    /// </summary>
    public void InsertIntegerPrimaryKeys(
        IReadOnlyList<(long Key, long RowId)> entries,
        string indexName,
        string tableName,
        string databaseName)
    {
        if (entries.Count == 0)
        {
            return;
        }

        var cacheKey = GetCacheKey(indexName, tableName, databaseName);
        ConcurrentInt64Int64Map map = _integerPrimaryKeyMaps.GetOrAdd(cacheKey, static _ => new ConcurrentInt64Int64Map());
        for (int i = 0; i < entries.Count; i++)
        {
            (long key, long rowId) = entries[i];
            map.Set(key, rowId); // reads stay lock-free
        }
    }

    /// <summary>
    /// Resolves the integer primary-key fast lane once, so batch validation can probe per row
    /// without re-hashing the cache key for every key. Returns <see langword="null"/> when the
    /// index has no fast lane (callers must fall back to the generic probe).
    /// </summary>
    internal ConcurrentInt64Int64Map? GetIntegerPrimaryKeyLane(string indexName, string tableName, string databaseName) =>
        _integerPrimaryKeyMaps.TryGetValue(GetCacheKey(indexName, tableName, databaseName), out ConcurrentInt64Int64Map? map)
            ? map
            : null;

    /// <summary>
    /// Inserts integer primary-key entries into the direct key -> row-id fast lane from parallel
    /// key/row-id arrays, without materializing a tuple list per batch.
    /// </summary>
    public void InsertIntegerPrimaryKeys(
        ReadOnlySpan<long> keys,
        ReadOnlySpan<long> rowIds,
        string indexName,
        string tableName,
        string databaseName)
    {
        if (keys.IsEmpty)
        {
            return;
        }

        var cacheKey = GetCacheKey(indexName, tableName, databaseName);
        ConcurrentInt64Int64Map map = _integerPrimaryKeyMaps.GetOrAdd(cacheKey, static _ => new ConcurrentInt64Int64Map());
        map.SetRange(keys, rowIds);
    }

    /// <summary>
    /// Inserts integer scalar-index entries into the direct key -> row-ids fast lane.
    /// </summary>
    public void InsertIntegerIndexEntries(
        IReadOnlyList<(long Key, long RowId)> entries,
        string indexName,
        string tableName,
        string databaseName)
    {
        if (entries.Count == 0)
        {
            return;
        }

        var cacheKey = GetCacheKey(indexName, tableName, databaseName);
        lock (_lock)
        {
            if (!_integerIndexMaps.TryGetValue(cacheKey, out Dictionary<long, List<long>>? map))
            {
                map = [];
                _integerIndexMaps[cacheKey] = map;
            }

            for (int i = 0; i < entries.Count; i++)
            {
                (long key, long rowId) = entries[i];
                if (!map.TryGetValue(key, out List<long>? rowIds))
                {
                    rowIds = [];
                    map[key] = rowIds;
                }

                rowIds.Add(rowId);
            }
        }
    }

    /// <summary>
    /// Inserts GUID primary-key entries into the direct key -> row-id fast lane.
    /// </summary>
    public void InsertGuidPrimaryKeys(
        IReadOnlyList<(Guid Key, long RowId)> entries,
        string indexName,
        string tableName,
        string databaseName)
    {
        if (entries.Count == 0)
        {
            return;
        }

        var cacheKey = GetCacheKey(indexName, tableName, databaseName);
        ConcurrentDictionary<Guid, long> map = _guidPrimaryKeyMaps.GetOrAdd(
            cacheKey,
            static _ => new ConcurrentDictionary<Guid, long>(GuidSimdEqualityComparer.Instance));
        for (int i = 0; i < entries.Count; i++)
        {
            (Guid key, long rowId) = entries[i];
            map[key] = rowId;
        }
    }

    /// <summary>
    /// Inserts GUID scalar-index entries into the direct key -> row-ids fast lane.
    /// </summary>
    public void InsertGuidIndexEntries(
        IReadOnlyList<(Guid Key, long RowId)> entries,
        string indexName,
        string tableName,
        string databaseName)
    {
        if (entries.Count == 0)
        {
            return;
        }

        var cacheKey = GetCacheKey(indexName, tableName, databaseName);
        lock (_lock)
        {
            if (!_guidIndexMaps.TryGetValue(cacheKey, out Dictionary<Guid, List<long>>? map))
            {
                map = new Dictionary<Guid, List<long>>(GuidSimdEqualityComparer.Instance);
                _guidIndexMaps[cacheKey] = map;
            }

            for (int i = 0; i < entries.Count; i++)
            {
                (Guid key, long rowId) = entries[i];
                if (!map.TryGetValue(key, out List<long>? rowIds))
                {
                    rowIds = [];
                    map[key] = rowIds;
                }

                rowIds.Add(rowId);
            }
        }
    }

    /// <summary>
    /// Returns whether the named index is backed by an integer primary-key fast lane.
    /// </summary>
    public bool HasIntegerPrimaryKeyFastLane(string indexName, string tableName, string databaseName)
    {
        return _integerPrimaryKeyMaps.ContainsKey(GetCacheKey(indexName, tableName, databaseName));
    }

    /// <summary>
    /// Returns whether the named index is backed by a single-column integer fast lane.
    /// </summary>
    public bool HasIntegerIndexFastLane(string indexName, string tableName, string databaseName)
    {
        var cacheKey = GetCacheKey(indexName, tableName, databaseName);
        lock (_lock)
        {
            return _integerIndexMaps.ContainsKey(cacheKey);
        }
    }

    /// <summary>
    /// Returns whether the named index is backed by a GUID primary-key fast lane.
    /// </summary>
    public bool HasGuidPrimaryKeyFastLane(string indexName, string tableName, string databaseName)
    {
        return _guidPrimaryKeyMaps.ContainsKey(GetCacheKey(indexName, tableName, databaseName));
    }

    /// <summary>
    /// Returns whether the named index is backed by a single-column GUID fast lane.
    /// </summary>
    public bool HasGuidIndexFastLane(string indexName, string tableName, string databaseName)
    {
        var cacheKey = GetCacheKey(indexName, tableName, databaseName);
        lock (_lock)
        {
            return _guidIndexMaps.ContainsKey(cacheKey);
        }
    }

    /// <summary>
    /// Removes an integer primary-key entry by key, in O(1). Used by the UPDATE path when a row's primary
    /// key changes; the unchanged-key case is handled by an upsert through
    /// <see cref="InsertIntegerPrimaryKeys(IReadOnlyList{ValueTuple{long, long}}, string, string, string)"/>.
    /// </summary>
    public void RemoveIntegerPrimaryKey(long key, string indexName, string tableName, string databaseName)
    {
        var cacheKey = GetCacheKey(indexName, tableName, databaseName);
        if (_integerPrimaryKeyMaps.TryGetValue(cacheKey, out ConcurrentInt64Int64Map? map))
        {
            map.TryRemove(key);
        }
    }

    /// <summary>
    /// Removes a GUID primary-key entry by key, in O(1).
    /// </summary>
    public void RemoveGuidPrimaryKey(Guid key, string indexName, string tableName, string databaseName)
    {
        var cacheKey = GetCacheKey(indexName, tableName, databaseName);
        if (_guidPrimaryKeyMaps.TryGetValue(cacheKey, out ConcurrentDictionary<Guid, long>? map))
        {
            map.TryRemove(key, out _);
        }
    }

    /// <summary>
    /// Removes a single (key, rowId) pair from the integer single-column fast lane, in O(bucket). Used by the
    /// UPDATE path so a moved row's stale row id is dropped from its key bucket before the new id is added.
    /// </summary>
    public void RemoveIntegerIndexEntry(long key, long rowId, string indexName, string tableName, string databaseName)
    {
        var cacheKey = GetCacheKey(indexName, tableName, databaseName);
        lock (_lock)
        {
            if (_integerIndexMaps.TryGetValue(cacheKey, out Dictionary<long, List<long>>? map)
                && map.TryGetValue(key, out List<long>? rowIds))
            {
                rowIds.Remove(rowId);
                if (rowIds.Count == 0)
                {
                    map.Remove(key);
                }
            }
        }
    }

    /// <summary>
    /// Removes a single (key, rowId) pair from the GUID single-column fast lane, in O(bucket).
    /// </summary>
    public void RemoveGuidIndexEntry(Guid key, long rowId, string indexName, string tableName, string databaseName)
    {
        var cacheKey = GetCacheKey(indexName, tableName, databaseName);
        lock (_lock)
        {
            if (_guidIndexMaps.TryGetValue(cacheKey, out Dictionary<Guid, List<long>>? map)
                && map.TryGetValue(key, out List<long>? rowIds))
            {
                rowIds.Remove(rowId);
                if (rowIds.Count == 0)
                {
                    map.Remove(key);
                }
            }
        }
    }

    /// <summary>
    /// Looks up an integer primary key without building a string key or traversing the generic BTree.
    /// </summary>
    public bool TryLookupIntegerPrimaryKey(
        long key,
        string indexName,
        string tableName,
        string databaseName,
        out long rowId)
    {
        var cacheKey = GetCacheKey(indexName, tableName, databaseName);
        // Lock-free: the hot point-lookup path must not serialize 8 concurrent readers on _lock.
        if (_integerPrimaryKeyMaps.TryGetValue(cacheKey, out ConcurrentInt64Int64Map? map)
            && map.TryGetValue(key, out rowId))
        {
            RuntimeQueryDiagnosticsScope.RecordIndexUse(indexName);
            return true;
        }

        rowId = 0;
        return false;
    }

    /// <summary>
    /// Looks up a GUID primary key without building a string key or traversing the generic BTree.
    /// </summary>
    public bool TryLookupGuidPrimaryKey(
        Guid key,
        string indexName,
        string tableName,
        string databaseName,
        out long rowId)
    {
        var cacheKey = GetCacheKey(indexName, tableName, databaseName);
        if (_guidPrimaryKeyMaps.TryGetValue(cacheKey, out ConcurrentDictionary<Guid, long>? map)
            && map.TryGetValue(key, out rowId))
        {
            RuntimeQueryDiagnosticsScope.RecordIndexUse(indexName);
            return true;
        }

        rowId = 0;
        return false;
    }

    /// <summary>
    /// Looks up a single-column integer index without building a string key or traversing the generic BTree.
    /// </summary>
    public IReadOnlyList<long> LookupIntegerIndex(
        long key,
        string indexName,
        string tableName,
        string databaseName)
    {
        return TryLookupIntegerIndex(key, indexName, tableName, databaseName, out IReadOnlyList<long>? rowIds)
            ? rowIds
            : Array.Empty<long>();
    }

    /// <summary>
    /// Attempts to look up a single-column integer index. Returns true when the index has an integer fast lane,
    /// even if the specific key has no rows.
    /// </summary>
    public bool TryLookupIntegerIndex(
        long key,
        string indexName,
        string tableName,
        string databaseName,
        out IReadOnlyList<long> rowIds)
    {
        var cacheKey = GetCacheKey(indexName, tableName, databaseName);
        lock (_lock)
        {
            if (_integerIndexMaps.TryGetValue(cacheKey, out Dictionary<long, List<long>>? map))
            {
                RuntimeQueryDiagnosticsScope.RecordIndexUse(indexName);
                rowIds = map.TryGetValue(key, out List<long>? rows)
                    ? rows
                    : Array.Empty<long>();
                return true;
            }
        }

        rowIds = Array.Empty<long>();
        return false;
    }

    /// <summary>
    /// Attempts to look up a single-column GUID index. Returns true when the index has a GUID fast lane,
    /// even if the specific key has no rows.
    /// </summary>
    public bool TryLookupGuidIndex(
        Guid key,
        string indexName,
        string tableName,
        string databaseName,
        out IReadOnlyList<long> rowIds)
    {
        var cacheKey = GetCacheKey(indexName, tableName, databaseName);
        lock (_lock)
        {
            if (_guidIndexMaps.TryGetValue(cacheKey, out Dictionary<Guid, List<long>>? map))
            {
                RuntimeQueryDiagnosticsScope.RecordIndexUse(indexName);
                rowIds = map.TryGetValue(key, out List<long>? rows)
                    ? rows
                    : Array.Empty<long>();
                return true;
            }
        }

        rowIds = Array.Empty<long>();
        return false;
    }

    /// <summary>
    /// Deletes row IDs from a scalar BTree index.
    /// </summary>
    public void DeleteFromIndex(List<long> toBeDeletedIds, string indexName, string tableName, string databaseName)
    {
        var cacheKey = GetCacheKey(indexName, tableName, databaseName);
        lock (_lock)
        {
            if (_integerPrimaryKeyMaps.ContainsKey(cacheKey))
            {
                DeleteIntegerPrimaryKeysByRowIdNoLock(cacheKey, toBeDeletedIds);
            }

            DeleteIntegerIndexRowsByRowIdNoLock(cacheKey, toBeDeletedIds);
            DeleteGuidPrimaryKeysByRowIdNoLock(cacheKey, toBeDeletedIds);
            DeleteGuidIndexRowsByRowIdNoLock(cacheKey, toBeDeletedIds);

            IIndex? index = TryGetScalarIndexNoLock(indexName, tableName, databaseName);
            if (index is null)
            {
                return;
            }

            index.DeleteValues(toBeDeletedIds);
            DeleteIntegerPrimaryKeysByRowIdNoLock(cacheKey, toBeDeletedIds);
            DeleteGuidPrimaryKeysByRowIdNoLock(cacheKey, toBeDeletedIds);
            MarkDirtyNoLock(cacheKey);
            FlushIfImmediate(cacheKey);
        }

        TrackMutation(cacheKey);
    }

    /// <summary>
    /// Performs point lookup via scalar BTree index.
    /// </summary>
    public IReadOnlyList<long> FilterUsingIndex(string columnValue, string indexName, string tableName, string databaseName)
    {
        if (TryParseIntegerPrimaryKey(columnValue, out long integerKey)
            && TryLookupIntegerPrimaryKey(integerKey, indexName, tableName, databaseName, out long rowId))
        {
            return [rowId];
        }

        if (TryParseGuidKey(columnValue, out Guid guidKey)
            && TryLookupGuidPrimaryKey(guidKey, indexName, tableName, databaseName, out long guidRowId))
        {
            return [guidRowId];
        }

        if (TryParseIntegerPrimaryKey(columnValue, out long integerIndexKey)
            && TryLookupIntegerIndex(integerIndexKey, indexName, tableName, databaseName, out IReadOnlyList<long> rowIds)
            && rowIds.Count > 0)
        {
            return rowIds;
        }

        if (TryParseGuidKey(columnValue, out Guid guidIndexKey)
            && TryLookupGuidIndex(guidIndexKey, indexName, tableName, databaseName, out IReadOnlyList<long> guidRowIds)
            && guidRowIds.Count > 0)
        {
            return guidRowIds;
        }

        IIndex index = GetOrLoadScalarIndex(indexName, tableName, databaseName);
        RuntimeQueryDiagnosticsScope.RecordIndexUse(indexName);
        return index.SearchReadOnly(columnValue);
    }

    /// <summary>
    /// Checks whether a scalar index contains a specific key.
    /// </summary>
    public bool IndexContainsKey(string key, string indexName, string tableName, string databaseName)
    {
        if (TryParseIntegerPrimaryKey(key, out long integerKey))
        {
            if (TryLookupIntegerPrimaryKey(integerKey, indexName, tableName, databaseName, out _))
            {
                return true;
            }
        }

        if (TryParseGuidKey(key, out Guid guidKey))
        {
            if (TryLookupGuidPrimaryKey(guidKey, indexName, tableName, databaseName, out _))
            {
                return true;
            }

        }

        IIndex index = GetOrLoadScalarIndex(indexName, tableName, databaseName);
        bool exists = index.ContainsKey(key);
        RuntimeQueryDiagnosticsScope.RecordIndexUse(indexName);
        return exists;
    }

    private void DeleteIntegerPrimaryKeysByRowIdNoLock(IndexCacheKey cacheKey, IReadOnlyList<long> rowIds)
    {
        if (!_integerPrimaryKeyMaps.TryGetValue(cacheKey, out ConcurrentInt64Int64Map? map) || map.IsEmpty)
        {
            return;
        }

        for (int i = 0; i < rowIds.Count; i++)
        {
            long rowId = rowIds[i];
            var search = new IntegerPrimaryKeySearch(rowId);
            map.ForEach(search, static (search, key, value) =>
            {
                if (value != search.RowId)
                {
                    return true;
                }

                search.Key = key;
                search.Found = true;
                return false;
            });

            if (search.Found)
            {
                map.TryRemove(search.Key);
            }
        }
    }

    private sealed class IntegerPrimaryKeySearch(long rowId)
    {
        public long RowId { get; } = rowId;
        public long Key { get; set; }
        public bool Found { get; set; }
    }

    private void DeleteGuidPrimaryKeysByRowIdNoLock(IndexCacheKey cacheKey, IReadOnlyList<long> rowIds)
    {
        if (!_guidPrimaryKeyMaps.TryGetValue(cacheKey, out ConcurrentDictionary<Guid, long>? map) || map.IsEmpty)
        {
            return;
        }

        for (int i = 0; i < rowIds.Count; i++)
        {
            long rowId = rowIds[i];
            Guid? keyToRemove = null;
            foreach (KeyValuePair<Guid, long> pair in map)
            {
                if (pair.Value == rowId)
                {
                    keyToRemove = pair.Key;
                    break;
                }
            }

            if (keyToRemove.HasValue)
            {
                map.TryRemove(keyToRemove.Value, out _);
            }
        }
    }

    private void DeleteIntegerIndexRowsByRowIdNoLock(IndexCacheKey cacheKey, IReadOnlyList<long> rowIds)
    {
        if (!_integerIndexMaps.TryGetValue(cacheKey, out Dictionary<long, List<long>>? map) || map.Count == 0)
        {
            return;
        }

        for (int i = 0; i < rowIds.Count; i++)
        {
            long rowId = rowIds[i];
            long? keyToRemove = null;
            foreach (KeyValuePair<long, List<long>> pair in map)
            {
                if (pair.Value.Remove(rowId) && pair.Value.Count == 0)
                {
                    keyToRemove = pair.Key;
                    break;
                }
            }

            if (keyToRemove.HasValue)
            {
                map.Remove(keyToRemove.Value);
            }
        }
    }

    private void DeleteGuidIndexRowsByRowIdNoLock(IndexCacheKey cacheKey, IReadOnlyList<long> rowIds)
    {
        if (!_guidIndexMaps.TryGetValue(cacheKey, out Dictionary<Guid, List<long>>? map) || map.Count == 0)
        {
            return;
        }

        for (int i = 0; i < rowIds.Count; i++)
        {
            long rowId = rowIds[i];
            Guid? keyToRemove = null;
            foreach (KeyValuePair<Guid, List<long>> pair in map)
            {
                if (pair.Value.Remove(rowId) && pair.Value.Count == 0)
                {
                    keyToRemove = pair.Key;
                    break;
                }
            }

            if (keyToRemove.HasValue)
            {
                map.Remove(keyToRemove.Value);
            }
        }
    }

    private IIndex? TryGetScalarIndexNoLock(string indexName, string tableName, string databaseName)
    {
        try
        {
            return GetOrLoadScalarIndex(indexName, tableName, databaseName);
        }
        catch (IndexException)
        {
            return null;
        }
    }

    private static bool TryParseIntegerPrimaryKey(string key, out long value)
    {
        if (long.TryParse(key, out value))
        {
            return true;
        }

        if (key.Length >= 3 && key[0] == '[' && key[^1] == ']')
        {
            return long.TryParse(key.AsSpan(1, key.Length - 2), out value);
        }

        value = 0;
        return false;
    }

    private static bool TryParseGuidKey(string key, out Guid value) =>
        Guid.TryParse(key, out value);

    /// <summary>
    /// Checks whether a scalar index references a row ID.
    /// </summary>
    public bool IndexContainsRow(long rowId, string indexName, string tableName, string databaseName)
    {
        IIndex index = GetOrLoadScalarIndex(indexName, tableName, databaseName);
        bool exists = index.ContainsValue(rowId);
        RuntimeQueryDiagnosticsScope.RecordIndexUse(indexName);
        return exists;
    }

    private IIndex GetOrLoadScalarIndex(string indexName, string tableName, string databaseName)
    {
        var cacheKey = GetCacheKey(indexName, tableName, databaseName);
        if (_cache.TryGetValue(cacheKey, out var cached) && cached is IIndex scalar)
        {
            return scalar;
        }

        IndexMetadata metadata = CreateScalarMetadata(indexName, tableName, databaseName);
        object? loaded = TryLoadIndex(metadata);
        if (loaded is IIndex loadedScalar)
        {
            return loadedScalar;
        }

        throw new IndexException($"Index {indexName} on table {tableName} does not exist!");
    }

    private static IndexMetadata CreateScalarMetadata(string indexName, string tableName, string databaseName)
    {
        return new IndexMetadata
        {
            IndexName = indexName,
            DatabaseName = databaseName,
            TableName = tableName,
            ColumnName = string.Empty,
            IndexType = "BTREE",
            PersistenceFormat = "json"
        };
    }

    private IVectorIndex GetOrLoadVectorIndex(string indexName, string tableName, string databaseName, string indexType)
    {
        var cacheKey = GetCacheKey(indexName, tableName, databaseName);
        if (_cache.TryGetValue(cacheKey, out var cached) && cached is IVectorIndex cachedIndex)
        {
            return cachedIndex;
        }

        if (!SupportsVectorIndexType(indexType))
        {
            throw new NotSupportedException($"Index type '{indexType}' is not registered as a vector index type.");
        }

        var metadata = CreateVectorMetadata(indexName, tableName, databaseName, indexType, metric: null);

        var loaded = TryLoadIndex(metadata);
        if (loaded is IVectorIndex vectorIndex)
        {
            return vectorIndex;
        }

        if (TryRebuildVectorIndexFromStorage(indexName, tableName, databaseName, indexType, out IVectorIndex? rebuilt, out Exception? rebuildError) && rebuilt != null)
        {
            return rebuilt;
        }

        if (rebuildError != null)
        {
            throw new IndexException(
                $"Vector index {indexName} on table {tableName} does not exist and rebuild failed for type '{indexType}': {rebuildError.Message}",
                rebuildError);
        }

        throw new IndexException($"Vector index {indexName} on table {tableName} does not exist or is incompatible with type '{indexType}'.");
    }

    private bool TryRebuildVectorIndexFromStorage(string indexName, string tableName, string databaseName, string indexType, out IVectorIndex? rebuilt, out Exception? rebuildError)
    {
        rebuilt = null;
        rebuildError = null;

        try
        {
            DataVoEngine engine = DataVoEngine.Current();
            var catalogIndex = engine.Catalog
                .GetTableIndexes(tableName, databaseName)
                .FirstOrDefault(index => index.IndexFileName.Equals(indexName, StringComparison.OrdinalIgnoreCase));

            if (catalogIndex == null || !SupportsVectorIndexType(catalogIndex.IndexKind) || catalogIndex.AttributeNames.Count != 1)
            {
                return false;
            }

            string vectorColumn = catalogIndex.AttributeNames[0];
            var rows = engine.StorageContext.GetTypedTableContents(tableName, databaseName);
            List<(long RowId, float[] Vector)> vectors = [];

            foreach (var row in rows)
            {
                // Vector columns deserialize directly to a Vector cell — read it typed, with no
                // per-row dictionary materialization or string re-coercion.
                if (!row.Value.TryGet(vectorColumn, out CellValue cell) || cell.IsNull || cell.Type != CellType.Vector)
                {
                    continue;
                }

                vectors.Add((row.Key, cell.AsVector()));
            }

            CreateVectorIndex(vectors, indexName, tableName, databaseName, indexType: catalogIndex.IndexKind);

            var cacheKey = GetCacheKey(indexName, tableName, databaseName);
            if (_cache.TryGetValue(cacheKey, out IIndexBase? cachedAfterRebuild) && cachedAfterRebuild is IVectorIndex rebuiltVector)
            {
                rebuilt = rebuiltVector;
                return true;
            }
        }
        catch (Exception ex)
        {
            rebuildError = ex;
        }

        return false;
    }

    private static IndexMetadata CreateVectorMetadata(string indexName, string tableName, string databaseName, string indexType, string? metric)
    {
        Dictionary<string, object> parameters = [];
        if (!string.IsNullOrWhiteSpace(metric))
        {
            parameters["metric"] = metric;
        }

        return new IndexMetadata
        {
            IndexName = indexName,
            DatabaseName = databaseName,
            TableName = tableName,
            ColumnName = string.Empty,
            IndexType = indexType.ToUpperInvariant(),
            PersistenceFormat = "json",
            Parameters = parameters,
        };
    }

    private static IIndexBase EnsureIndexBase(object index, string indexType, IndexCacheKey cacheKey)
    {
        if (index is IIndexBase typed)
        {
            return typed;
        }

        throw new IndexException($"Index type '{indexType}' returned non-index instance '{index.GetType().FullName}' for cache key '{cacheKey}'.");
    }
}
