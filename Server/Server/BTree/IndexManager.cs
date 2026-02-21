using Server.Server.BTree.Core;
using Server.Server.BTree.Binary;

namespace Server.Server.BTree;

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
    private readonly Dictionary<string, IIndex> _cache = new();

    private IndexManager() { }

    public static IndexManager Instance
    {
        get
        {
            _instance ??= new IndexManager();
            return _instance;
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
                index = BinaryBTreeIndex.LoadFile(filePath);
            }

            _cache[cacheKey] = index;
            return index;
        }

        throw new Exception($"Index {indexName} on table {tableName} does not exist!");
    }

    /// <summary>
    /// Create a new index and bulk-insert initial values.
    /// values is a dictionary mapping index key → list of row IDs.
    /// </summary>
    public void CreateIndex(Dictionary<string, List<string>> values, string indexName, string tableName, string databaseName, IndexType indexType = IndexType.JsonBTree)
    {
        string cacheKey = GetCacheKey(indexName, tableName, databaseName);
        string filePath = GetIndexFilePath(indexName, tableName, databaseName);

        IIndex index = indexType == IndexType.BinaryBTree 
            ? new BinaryBTreeIndex() 
            : new JsonBTreeIndex();
            
        // Initial setup for binary pagers required before inserting
        if (index is BinaryBTreeIndex binIndex)
        {
            binIndex.Load(filePath);
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
    }

    /// <summary>
    /// Drop an index — remove from cache and delete the .btree file.
    /// </summary>
    public void DropIndex(string indexName, string tableName, string databaseName)
    {
        string cacheKey = GetCacheKey(indexName, tableName, databaseName);
        string filePath = GetIndexFilePath(indexName, tableName, databaseName);

        _cache.Remove(cacheKey);

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
        index.Save(GetIndexFilePath(indexName, tableName, databaseName));
    }

    /// <summary>
    /// Delete row IDs from an index.
    /// Removes all entries containing any of the specified row IDs.
    /// </summary>
    public void DeleteFromIndex(List<string> toBeDeletedIds, string indexName, string tableName, string databaseName)
    {
        var index = GetOrLoad(indexName, tableName, databaseName);
        index.DeleteValues(toBeDeletedIds);
        index.Save(GetIndexFilePath(indexName, tableName, databaseName));
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
}
