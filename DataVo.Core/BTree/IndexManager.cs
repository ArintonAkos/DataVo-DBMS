using DataVo.Core.BTree.Core;
using DataVo.Core.BTree.Binary;
using DataVo.Core.BTree.BPlus;

namespace DataVo.Core.BTree;

public enum IndexPersistenceMode
{
    Immediate,
    Buffered,
}

/// <summary>
/// Singleton manager for all active B-Tree indexes.
/// Provides the same interface previously offered by DbContext's index methods.
/// Indexes are cached in memory and lazily loaded from disk.
/// </summary>
public class IndexManager
{
    private static IndexManager? _instance;
    private const string DatabasesDir = "databases";

    /// <summary>
    /// In-memory cache of loaded indexes, keyed by "{dbName}/{tableName}_{indexName}".
    /// </summary>
    private readonly Dictionary<string, IIndex> _cache = [];
    private readonly Dictionary<string, string> _cacheFilePaths = [];
    private readonly HashSet<string> _dirtyIndexes = [];
    private readonly Dictionary<string, int> _pendingMutationCounts = [];
    private readonly Lock _persistenceLock = new();

    private IndexPersistenceMode _persistenceMode = IndexPersistenceMode.Immediate;
    // Specifies the number of mutations that must occur before an index is flushed to disk.
    private int _flushMutationThreshold = 256;

    private IndexManager() { }

    public static IndexManager Instance
    {
        get
        {
            _instance ??= new IndexManager();
            return _instance;
        }
    }

    public void ConfigurePersistence(IndexPersistenceMode mode, int flushMutationThreshold = 256)
    {
        if (flushMutationThreshold <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(flushMutationThreshold), "Flush threshold must be greater than zero.");
        }

        lock (_persistenceLock)
        {
            _persistenceMode = mode;
            _flushMutationThreshold = flushMutationThreshold;
        }

        if (mode == IndexPersistenceMode.Immediate)
        {
            FlushDirtyIndexes();
        }
    }

    public void FlushDirtyIndexes()
    {
        List<string> dirtyKeys;

        lock (_persistenceLock)
        {
            dirtyKeys = _dirtyIndexes.ToList();
        }

        foreach (var cacheKey in dirtyKeys)
        {
            if (!_cache.TryGetValue(cacheKey, out var index))
            {
                continue;
            }

            if (!_cacheFilePaths.TryGetValue(cacheKey, out string? filePath))
            {
                continue;
            }

            index.Save(filePath);

            lock (_persistenceLock)
            {
                _dirtyIndexes.Remove(cacheKey);
                _pendingMutationCounts.Remove(cacheKey);
            }
        }
    }

    /// <summary>
    /// Build the file path for a given index.
    /// </summary>
    private static string GetIndexFilePath(string indexName, string tableName, string databaseName)
    {
        return Path.Combine(DatabasesDir, databaseName, $"{tableName}_{indexName}_index.btree");
    }

    /// <summary>
    /// Build the cache key for a given index.
    /// </summary>
    private static string GetCacheKey(string indexName, string tableName, string databaseName)
    {
        return $"{databaseName}/{tableName}_{indexName}";
    }

    /// <summary>
    /// Get or lazily load an index from disk.
    /// </summary>
    private IIndex GetOrLoad(string indexName, string tableName, string databaseName)
    {
        string cacheKey = GetCacheKey(indexName, tableName, databaseName);

        if (_cache.TryGetValue(cacheKey, out IIndex? cached))
        {
            return cached;
        }

        string filePath = GetIndexFilePath(indexName, tableName, databaseName);
        if (File.Exists(filePath))
        {
            IIndex index;
            // Hacky detection for benchmark vs standard to keep tests passing.
            // A real engine would persist metadata for table's `IndexType`.
            if (File.ReadAllText(filePath).StartsWith("{"))
            {
                index = JsonBTreeIndex.Load(filePath);
            }
            else
            {
                // Hacky detection: if we assume B+Tree is the default binary engine now
                index = BinaryBPlusTreeIndex.LoadFile(filePath);
            }

            _cache[cacheKey] = index;
            _cacheFilePaths[cacheKey] = filePath;
            return index;
        }

        throw new Exception($"Index {indexName} on table {tableName} does not exist!");
    }

    /// <summary>
    /// The default index type to use when a specific type is not provided.
    /// Defaults to B+Tree if not specified.
    /// </summary>
    public IndexType DefaultIndexType { get; set; } = IndexType.BinaryBPlusTree;

    /// <summary>
    /// Create a new index and bulk-insert initial values.
    /// values is a dictionary mapping index key → list of row IDs.
    /// </summary>
    public void CreateIndex(Dictionary<string, List<string>> values, string indexName, string tableName, string databaseName, IndexType? indexType = null)
    {
        string cacheKey = GetCacheKey(indexName, tableName, databaseName);
        string filePath = GetIndexFilePath(indexName, tableName, databaseName);

        IndexType typeToUse = indexType ?? DefaultIndexType;

        IIndex index = typeToUse switch
        {
            IndexType.BinaryBPlusTree => new BinaryBPlusTreeIndex(),
            IndexType.BinaryBTree => new BinaryBTreeIndex(),
            _ => new JsonBTreeIndex()
        };

        // Initial setup for binary pagers required before inserting
        if (index is BinaryBTreeIndex binIndex)
        {
            binIndex.Load(filePath);
        }
        else if (index is BinaryBPlusTreeIndex bplusIndex)
        {
            bplusIndex.Load(filePath);
        }

        foreach (var kvp in values)
        {
            foreach (string rowId in kvp.Value)
            {
                index.Insert(kvp.Key, rowId);
            }
        }

        index.Save(filePath);
        _cache[cacheKey] = index;
        _cacheFilePaths[cacheKey] = filePath;
    }

    /// <summary>
    /// Drop an index — remove from cache and delete the .btree file.
    /// </summary>
    public void DropIndex(string indexName, string tableName, string databaseName)
    {
        string cacheKey = GetCacheKey(indexName, tableName, databaseName);
        string filePath = GetIndexFilePath(indexName, tableName, databaseName);

        _cache.Remove(cacheKey);
        _cacheFilePaths.Remove(cacheKey);

        lock (_persistenceLock)
        {
            _dirtyIndexes.Remove(cacheKey);
            _pendingMutationCounts.Remove(cacheKey);
        }

        if (File.Exists(filePath))
        {
            File.Delete(filePath);
        }
    }

    /// <summary>
    /// Insert a single key-value pair into an existing index.
    /// </summary>
    public void InsertIntoIndex(string value, string rowId, string indexName, string tableName, string databaseName)
    {
        var index = GetOrLoad(indexName, tableName, databaseName);
        index.Insert(value, rowId);
        PersistAfterMutation(index, indexName, tableName, databaseName);
    }

    /// <summary>
    /// Delete row IDs from an index.
    /// Removes all entries containing any of the specified row IDs.
    /// </summary>
    public void DeleteFromIndex(List<string> toBeDeletedIds, string indexName, string tableName, string databaseName)
    {
        var index = GetOrLoad(indexName, tableName, databaseName);
        index.DeleteValues(toBeDeletedIds);
        PersistAfterMutation(index, indexName, tableName, databaseName);
    }

    /// <summary>
    /// Look up row IDs by index key value. Returns matching row IDs as a HashSet.
    /// </summary>
    public HashSet<string> FilterUsingIndex(string columnValue, string indexName, string tableName, string databaseName)
    {
        var index = GetOrLoad(indexName, tableName, databaseName);
        return index.Search(columnValue).ToHashSet();
    }

    /// <summary>
    /// Check if the index contains any entry for the given key value.
    /// </summary>
    public bool IndexContainsRow(string rowId, string indexName, string tableName, string databaseName)
    {
        var index = GetOrLoad(indexName, tableName, databaseName);
        return index.ContainsValue(rowId);
    }

    private void PersistAfterMutation(IIndex index, string indexName, string tableName, string databaseName)
    {
        string cacheKey = GetCacheKey(indexName, tableName, databaseName);
        string filePath = GetIndexFilePath(indexName, tableName, databaseName);

        if (_persistenceMode == IndexPersistenceMode.Immediate)
        {
            index.Save(filePath);
            return;
        }

        bool shouldFlush;
        lock (_persistenceLock)
        {
            _dirtyIndexes.Add(cacheKey);

            if (!_pendingMutationCounts.TryGetValue(cacheKey, out int pendingMutations))
            {
                pendingMutations = 0;
            }

            pendingMutations++;
            _pendingMutationCounts[cacheKey] = pendingMutations;
            shouldFlush = pendingMutations >= _flushMutationThreshold;
        }

        if (shouldFlush)
        {
            index.Save(filePath);
            lock (_persistenceLock)
            {
                _dirtyIndexes.Remove(cacheKey);
                _pendingMutationCounts.Remove(cacheKey);
            }
        }
    }

}
