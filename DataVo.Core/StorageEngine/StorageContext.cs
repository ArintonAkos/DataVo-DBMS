using DataVo.Core.StorageEngine.Config;
using DataVo.Core.StorageEngine.Disk;
using DataVo.Core.StorageEngine.Memory;
using DataVo.Core.StorageEngine.Serialization;

namespace DataVo.Core.StorageEngine;

/// <summary>
/// The primary Data Access Object for the engine. Replaces the old MongoDB 'DbContext'.
/// All SQL operations route through this context to interact with the underlying byte storage.
/// </summary>
public class StorageContext(DataVoConfig config)
{
    private readonly IStorageEngine _storageEngine = config.StorageMode switch
    {
        StorageMode.InMemory => new InMemoryStorageEngine(),
        StorageMode.Disk => new DiskStorageEngine(config.DiskStoragePath ?? "./datavo_data"),
        _ => throw new ArgumentOutOfRangeException()
    };
    private static StorageContext? _instance;

    /// <summary>
    /// For legacy singleton usage during benchmarks, defaults to InMemory if unconfigured.
    /// </summary>
    public static StorageContext Instance
    {
        get
        {
            _instance ??= new StorageContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
            return _instance;
        }
    }

    /// <summary>
    /// Explicitly initializes the global DAO configuration. Extremely useful for testing different Storage Modes.
    /// </summary>
    public static void Initialize(DataVoConfig config)
    {
        _instance = new StorageContext(config);
    }

    public void CreateTable(string tableName, string databaseName)
    {
        // For purely binary engines without explicit Schema-file logic yet, 
        // creating a table doesn't mandate allocating a physical file immediately.
        // It will be lazily created on the first INSERT inside DiskStorageEngine.
    }

    public void DropTable(string tableName, string databaseName)
    {
        _storageEngine.DropTable(databaseName, tableName);
    }

    public long InsertOneIntoTable(Dictionary<string, dynamic> row, string tableName, string databaseName)
    {
        byte[] serializedData = RowSerializer.Serialize(databaseName, tableName, row);
        return _storageEngine.InsertRow(databaseName, tableName, serializedData);
    }

    public List<long> InsertIntoTable(List<Dictionary<string, dynamic>> rows, string tableName, string databaseName)
    {
        if (rows.Count == 0) return [];

        var serializedRows = new List<byte[]>(rows.Count);
        foreach (var row in rows)
        {
            serializedRows.Add(RowSerializer.Serialize(databaseName, tableName, row));
        }

        return _storageEngine.InsertRows(databaseName, tableName, serializedRows);
    }

    public void DeleteFromTable(List<long> toBeDeletedIds, string tableName, string databaseName)
    {
        foreach (long id in toBeDeletedIds)
        {
            _storageEngine.DeleteRow(databaseName, tableName, id);
        }
    }

    public bool TableContainsRow(long rowId, string tableName, string databaseName)
    {
        try
        {
            // Simple validation fetch. Memory mapped fetching makes this cheap.
            var bytes = _storageEngine.ReadRow(databaseName, tableName, rowId);
            return bytes.Length > 0;
        }
        catch (Exception)
        {
            return false;
        }
    }

    // --- Complex Read Implementations ---

    public Dictionary<long, Dictionary<string, dynamic>> SelectFromTable(List<long>? ids, List<string> requestedColumns,
        string tableName, string databaseName)
    {
        // 1. Fetch raw rows
        Dictionary<long, Dictionary<string, dynamic>> allSelectedData = GetTableContents(ids, tableName, databaseName);

        // 2. Filter down to only requested columns (Projection)
        if (requestedColumns.Count != 0)
        {
            var normalizedColumns = requestedColumns
                .Select(c => c.Contains('.') ? c.Substring(c.LastIndexOf('.') + 1) : c)
                .ToList();

            foreach (var rowEntry in allSelectedData)
            {
                var row = rowEntry.Value;
                var keysToRemove = row.Keys.Except(normalizedColumns).ToList();
                foreach (string keyToRemove in keysToRemove)
                {
                    row.Remove(keyToRemove);
                }
            }
        }

        return allSelectedData;
    }

    /// <summary>
    /// Fetches all records from a table, decrypting them from bytes back to typed dictionaries.
    /// </summary>
    public Dictionary<long, Dictionary<string, dynamic>> GetTableContents(string tableName, string databaseName)
    {
        return GetTableContents(null, tableName, databaseName);
    }

    /// <summary>
    /// Fetches specific records (via IDs/RowIds) from a table, decrypting them from bytes back to typed dictionaries.
    /// Used heavily by Index-driven querying (B+Tree).
    /// </summary>
    public Dictionary<long, Dictionary<string, dynamic>> GetTableContents(List<long>? rowIds, string tableName, string databaseName)
    {
        var parsedTableData = new Dictionary<long, Dictionary<string, dynamic>>();

        if (rowIds != null)
        {
            // Indexed point-queries (Fetch explicit RowIds from Disk)
            if (rowIds.Count == 0) return parsedTableData;

            foreach (long rowId in rowIds)
            {
                byte[] rawRow = _storageEngine.ReadRow(databaseName, tableName, rowId);
                parsedTableData[rowId] = RowSerializer.Deserialize(databaseName, tableName, rawRow);
            }
        }
        else
        {
            // Full Table Scan (Fetch all active rows on Disk)
            foreach (var rowTuple in _storageEngine.ReadAllRows(databaseName, tableName))
            {
                parsedTableData[rowTuple.RowId] = RowSerializer.Deserialize(databaseName, tableName, rowTuple.RawRow);
            }
        }

        return parsedTableData;
    }

    public void DropDatabase(string databaseName)
    {
        // The simple generic implementation doesn't specifically manage dropping an entire DB folder yet.
        // Needs a Directory.Delete on DiskStorage if enabled, or sweeping the MemoryStorage maps.
    }
}
