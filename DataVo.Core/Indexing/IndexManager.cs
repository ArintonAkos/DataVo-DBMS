using DataVo.Core.BTree;
using DataVo.Core.BTree.Core;
using System.Globalization;
using DataVo.Core.Exceptions;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.Utils;
using DataVo.Core.Indexing.BTree;
using DataVo.Core.Indexing.HNSW;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Diagnostics;

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
        "HNSW"
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
    private readonly Dictionary<string, IIndexBase> _cache = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Metadata storage: maps CacheKey -> index metadata.
    /// </summary>
    private readonly Dictionary<string, IndexMetadata> _metadata = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Tracks paths for cache entries: maps CacheKey -> file path.
    /// </summary>
    private readonly Dictionary<string, string> _cachePaths = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Tracks dirty (modified) indices that need flushing.
    /// </summary>
    private readonly HashSet<string> _dirtyIndices = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Tracks mutation count per index for buffered persistence.
    /// </summary>
    private readonly Dictionary<string, int> _pendingMutations = new(StringComparer.OrdinalIgnoreCase);

    private readonly Lock _lock = new();
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
            IIndexBase index = EnsureIndexBase(rawIndex, metadata.IndexType, metadata.CacheKey);

            _cache[metadata.CacheKey] = index;
            _metadata[metadata.CacheKey] = metadata;
            _dirtyIndices.Add(metadata.CacheKey);

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
            if (_cache.TryGetValue(metadata.CacheKey, out IIndexBase? cached))
                return cached;

            var type = metadata.IndexType.ToUpper();
            if (!_persistenceHandlers.TryGetValue(type, out var persistence))
                throw new NotSupportedException($"Index type '{metadata.IndexType}' has no persistence handler");

            var filePath = overridePath ?? BuildIndexPath(metadata);
            if (!persistence.FileExists(filePath))
                return null;

            object rawIndex = persistence.LoadIndex(filePath);
            IIndexBase index = EnsureIndexBase(rawIndex, metadata.IndexType, metadata.CacheKey);
            _cache[metadata.CacheKey] = index;
            _metadata[metadata.CacheKey] = metadata;
            _cachePaths[metadata.CacheKey] = filePath;

            return index;
        }
    }

    /// <summary>
    /// Marks an index as dirty (requiring flush to disk).
    /// </summary>
    public void MarkDirty(string cacheKey)
    {
        lock (_lock)
            MarkDirtyNoLock(cacheKey);

        TrackMutation(cacheKey);
    }

    private void MarkDirtyNoLock(string cacheKey)
    {
        _dirtyIndices.Add(cacheKey);
    }

    /// <summary>
    /// Tracks a mutation and flushes if threshold exceeded (buffered mode).
    /// </summary>
    private void TrackMutation(string cacheKey)
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

    private void FlushInternal(string cacheKey)
    {
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
    public IIndexBase? TryGetCached(string cacheKey)
    {
        lock (_lock)
            return _cache.TryGetValue(cacheKey, out var index) ? index : null;
    }

    /// <summary>
    /// Gets metadata for an index.
    /// </summary>
    public IndexMetadata? TryGetMetadata(string cacheKey)
    {
        lock (_lock)
            return _metadata.TryGetValue(cacheKey, out var meta) ? meta : null;
    }

    /// <summary>
    /// Removes an index from cache and optionally deletes its disk file.
    /// </summary>
    public void RemoveIndex(string cacheKey, bool deleteFile = false)
    {
        lock (_lock)
        {
            _metadata.TryGetValue(cacheKey, out var metadata);

            _cache.Remove(cacheKey);
            _metadata.Remove(cacheKey);
            _dirtyIndices.Remove(cacheKey);
            _pendingMutations.Remove(cacheKey);

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

        _cachePaths[metadata.CacheKey] = BuildIndexPath(metadata);
        MarkDirty(metadata.CacheKey);
        FlushInternal(metadata.CacheKey);
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
        string cacheKey = GetCacheKey(indexName, tableName, databaseName);
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

        List<string> keysToRemove;
        lock (_lock)
        {
            keysToRemove = _cache.Keys
                .Where(cacheKey => cacheKey.StartsWith($"{databaseName}/", StringComparison.OrdinalIgnoreCase))
                .ToList();
        }

        foreach (string cacheKey in keysToRemove)
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
    }

    private static string GetCacheKey(string indexName, string tableName, string databaseName)
    {
        return $"{databaseName}/{tableName}_{indexName}".ToLowerInvariant();
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
        string cacheKey = GetCacheKey(indexName, tableName, databaseName);
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
            vectorIndex.Insert(rowId, [.. vector]);
        }

        _cachePaths[cacheKey] = BuildIndexPath(metadata);
        MarkDirty(cacheKey);
        FlushInternal(cacheKey);
    }

    /// <summary>
    /// Inserts or updates a single vector in an existing vector index.
    /// </summary>
    public void InsertIntoVectorIndex(float[] vector, long rowId, string indexName, string tableName, string databaseName, string indexType = "HNSW")
    {
        string cacheKey = GetCacheKey(indexName, tableName, databaseName);
        var vectorIndex = GetOrLoadVectorIndex(indexName, tableName, databaseName, indexType);
        vectorIndex.Insert(rowId, [.. vector]);
        MarkDirty(cacheKey);
        FlushInternal(cacheKey);
    }

    /// <summary>
    /// Deletes vectors by row IDs.
    /// </summary>
    public void DeleteFromVectorIndex(List<long> toBeDeletedIds, string indexName, string tableName, string databaseName, string indexType = "HNSW")
    {
        string cacheKey = GetCacheKey(indexName, tableName, databaseName);
        var vectorIndex = GetOrLoadVectorIndex(indexName, tableName, databaseName, indexType);
        vectorIndex.Delete(toBeDeletedIds);
        MarkDirty(cacheKey);
        FlushInternal(cacheKey);
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
            string cacheKey = GetCacheKey(indexName, tableName, databaseName);
            IIndex index = GetOrLoadScalarIndex(indexName, tableName, databaseName);
            index.Insert(value, rowId);
            MarkDirtyNoLock(cacheKey);
            FlushInternal(cacheKey);
        }

        TrackMutation(GetCacheKey(indexName, tableName, databaseName));
    }

    /// <summary>
    /// Deletes row IDs from a scalar BTree index.
    /// </summary>
    public void DeleteFromIndex(List<long> toBeDeletedIds, string indexName, string tableName, string databaseName)
    {
        lock (_lock)
        {
            string cacheKey = GetCacheKey(indexName, tableName, databaseName);
            IIndex index = GetOrLoadScalarIndex(indexName, tableName, databaseName);
            index.DeleteValues(toBeDeletedIds);
            MarkDirtyNoLock(cacheKey);
            FlushInternal(cacheKey);
        }

        TrackMutation(GetCacheKey(indexName, tableName, databaseName));
    }

    /// <summary>
    /// Performs point lookup via scalar BTree index.
    /// </summary>
    public HashSet<long> FilterUsingIndex(string columnValue, string indexName, string tableName, string databaseName)
    {
        IIndex index = GetOrLoadScalarIndex(indexName, tableName, databaseName);
        HashSet<long> results = [.. index.Search(columnValue)];
        RuntimeQueryDiagnosticsScope.RecordIndexUse(indexName);
        return results;
    }

    /// <summary>
    /// Checks whether a scalar index contains a specific key.
    /// </summary>
    public bool IndexContainsKey(string key, string indexName, string tableName, string databaseName)
    {
        IIndex index = GetOrLoadScalarIndex(indexName, tableName, databaseName);
        bool exists = index.Search(key).Any();
        RuntimeQueryDiagnosticsScope.RecordIndexUse(indexName);
        return exists;
    }

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
        string cacheKey = GetCacheKey(indexName, tableName, databaseName);
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
        string cacheKey = GetCacheKey(indexName, tableName, databaseName);
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
            var rows = engine.StorageContext.GetTableContents(tableName, databaseName);
            List<(long RowId, float[] Vector)> vectors = [];

            foreach (var row in rows)
            {
                if (!row.Value.TryGetValue(vectorColumn, out dynamic? rawValue) || rawValue == null)
                {
                    continue;
                }

                if (TryCoerceVector(rawValue, out float[] vector))
                {
                    vectors.Add((row.Key, vector));
                }
            }

            CreateVectorIndex(vectors, indexName, tableName, databaseName, indexType: catalogIndex.IndexKind);

            string cacheKey = GetCacheKey(indexName, tableName, databaseName);
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

    private static bool TryCoerceVector(object value, out float[] vector)
    {
        if (value is float[] typed)
        {
            vector = [.. typed];
            return true;
        }

        if (value is IEnumerable<float> floatEnumerable)
        {
            vector = [.. floatEnumerable];
            return true;
        }

        if (value is string text)
        {
            string trimmed = text.Trim();
            if (trimmed.StartsWith('[') && trimmed.EndsWith(']') && trimmed.Length >= 2)
            {
                string[] parts = trimmed[1..^1]
                    .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

                float[] parsed = new float[parts.Length];
                for (int i = 0; i < parts.Length; i++)
                {
                    if (!float.TryParse(parts[i], NumberStyles.Float, CultureInfo.InvariantCulture, out parsed[i]))
                    {
                        vector = [];
                        return false;
                    }
                }

                vector = parsed;
                return true;
            }
        }

        vector = [];
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

    private static IIndexBase EnsureIndexBase(object index, string indexType, string cacheKey)
    {
        if (index is IIndexBase typed)
        {
            return typed;
        }

        throw new IndexException($"Index type '{indexType}' returned non-index instance '{index.GetType().FullName}' for cache key '{cacheKey}'.");
    }
}
