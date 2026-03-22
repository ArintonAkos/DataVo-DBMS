using System.Text.Json;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.Utils;

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
public class IndexManagerV2 : IDisposable
{
    private static IndexManagerV2? _instance;
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
    private readonly Dictionary<string, object> _cache = [];

    /// <summary>
    /// Metadata storage: maps CacheKey -> index metadata.
    /// </summary>
    private readonly Dictionary<string, IndexMetadata> _metadata = [];

    /// <summary>
    /// Tracks paths for cache entries: maps CacheKey -> file path.
    /// </summary>
    private readonly Dictionary<string, string> _cachePaths = [];

    /// <summary>
    /// Tracks dirty (modified) indices that need flushing.
    /// </summary>
    private readonly HashSet<string> _dirtyIndices = [];

    /// <summary>
    /// Tracks mutation count per index for buffered persistence.
    /// </summary>
    private readonly Dictionary<string, int> _pendingMutations = [];

    private readonly Lock _lock = new();
    private IndexPersistenceMode _persistenceMode = IndexPersistenceMode.Immediate;
    private int _flushMutationThreshold = 256;

    public IndexManagerV2()
        : this(config: null, engineStorageRoot: null)
    {
    }

    public IndexManagerV2(DataVoConfig? config, string? engineStorageRoot)
    {
        _indexRootDirectory = ResolveIndexRootDirectory(config, engineStorageRoot);
        Directory.CreateDirectory(_indexRootDirectory);
    }

    /// <summary>
    /// Gets the singleton instance (for backward compatibility).
    /// </summary>
    public static IndexManagerV2 Instance
    {
        get
        {
            _instance ??= new IndexManagerV2();
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
    /// Creates a new index of the specified type.
    /// </summary>
    public object CreateIndex(string indexType, IndexMetadata metadata, Dictionary<string, object> @params)
    {
        lock (_lock)
        {
            var type = indexType.ToUpper();
            if (!_factories.TryGetValue(type, out var factory))
                throw new NotSupportedException($"Index type '{indexType}' not registered");

            var index = factory.CreateIndex(metadata.IndexName, metadata.ColumnName, @params);
            
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
            if (_cache.TryGetValue(metadata.CacheKey, out var cached))
                return cached;

            var type = metadata.IndexType.ToUpper();
            if (!_persistenceHandlers.TryGetValue(type, out var persistence))
                throw new NotSupportedException($"Index type '{metadata.IndexType}' has no persistence handler");

            var filePath = overridePath ?? BuildIndexPath(metadata);
            if (!persistence.FileExists(filePath))
                return null;

            var index = persistence.LoadIndex(filePath);
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
            _dirtyIndices.Add(cacheKey);

        TrackMutation(cacheKey);
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
        if (!_cache.TryGetValue(cacheKey, out var index))
            return;
        if (!_metadata.TryGetValue(cacheKey, out var metadata))
            return;

        var type = metadata.IndexType.ToUpper();
        if (!_persistenceHandlers.TryGetValue(type, out var persistence))
            return;

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
    public object? TryGetCached(string cacheKey)
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
            _cache.Remove(cacheKey);
            _metadata.Remove(cacheKey);
            _dirtyIndices.Remove(cacheKey);
            _pendingMutations.Remove(cacheKey);

            if (deleteFile && _cachePaths.TryGetValue(cacheKey, out var path))
            {
                try { File.Delete(path); }
                catch { /* Ignore deletion errors */ }
            }

            _cachePaths.Remove(cacheKey);
        }
    }

    /// <summary>
    /// Sets persistence mode and mutation threshold.
    /// </summary>
    public void SetPersistenceMode(IndexPersistenceMode mode, int flushThreshold = 256)
    {
        lock (_lock)
        {
            _persistenceMode = mode;
            _flushMutationThreshold = flushThreshold;
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

        var dir = Path.Combine(_indexRootDirectory, metadata.DatabaseName, metadata.TableName);
        Directory.CreateDirectory(dir);

        var filename = $"{metadata.IndexName}{persistence.FileExtension}";
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

    public void Dispose()
    {
        FlushAll();
    }
}
