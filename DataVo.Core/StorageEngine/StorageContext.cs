using DataVo.Core.StorageEngine.Config;
using DataVo.Core.StorageEngine.Disk;
using DataVo.Core.StorageEngine.Memory;
using DataVo.Core.StorageEngine.Serialization;
using DataVo.Core.Runtime;
using DataVo.Core.Transactions;

namespace DataVo.Core.StorageEngine;

/// <summary>
/// The primary Data Access Object for the engine. Replaces the old MongoDB 'DbContext'.
/// All SQL operations route through this context to interact with the underlying byte storage.
/// </summary>
/// <example>
/// <code>
/// StorageContext.Initialize(new DataVoConfig { StorageMode = StorageMode.InMemory });
/// StorageContext context = StorageContext.Instance;
/// context.InsertOneIntoTable(new Dictionary&lt;string, dynamic&gt; { ["Id"] = 1 }, "Users", "DemoDb");
/// </code>
/// </example>
public class StorageContext(DataVoConfig config)
{
    /// <summary>
    /// Gets the configuration that created this context.
    /// </summary>
    public DataVoConfig Config { get; } = config;

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

        DataVoEngine.ResetCurrent(_instance);

        if (config.StorageMode == StorageMode.Disk && config.WalEnabled)
        {
            new RecoveryManager(config).Recover();
        }
    }

    /// <summary>
    /// Creates a logical table container in the active storage engine.
    /// </summary>
    /// <param name="tableName">The table to create.</param>
    /// <param name="databaseName">The owning database.</param>
    public void CreateTable(string tableName, string databaseName)
    {
        // For purely binary engines without explicit Schema-file logic yet, 
        // creating a table doesn't mandate allocating a physical file immediately.
        // It will be lazily created on the first INSERT inside DiskStorageEngine.
    }

    /// <summary>
    /// Drops a table and its physical storage payload.
    /// </summary>
    /// <param name="tableName">The table to drop.</param>
    /// <param name="databaseName">The owning database.</param>
    public void DropTable(string tableName, string databaseName)
    {
        _storageEngine.DropTable(databaseName, tableName);
    }

    /// <summary>
    /// Serializes and inserts a single row into a table.
    /// </summary>
    /// <param name="row">The row values keyed by column name.</param>
    /// <param name="tableName">The target table.</param>
    /// <param name="databaseName">The owning database.</param>
    /// <returns>The assigned row identifier.</returns>
    public long InsertOneIntoTable(Dictionary<string, dynamic> row, string tableName, string databaseName)
    {
        byte[] serializedData = RowSerializer.Serialize(databaseName, tableName, row);
        return _storageEngine.InsertRow(databaseName, tableName, serializedData);
    }

    /// <summary>
    /// Serializes and inserts multiple rows into a table.
    /// </summary>
    /// <param name="rows">The rows to insert.</param>
    /// <param name="tableName">The target table.</param>
    /// <param name="databaseName">The owning database.</param>
    /// <returns>The assigned row identifiers.</returns>
    public List<long> InsertIntoTable(List<Dictionary<string, dynamic>> rows, string tableName, string databaseName)
    {
        if (rows.Count == 0) return [];

        return _storageEngine.InsertRows(databaseName, tableName, SerializeRows(rows, tableName, databaseName));
    }

    /// <summary>
    /// Deletes the specified row identifiers from a table.
    /// </summary>
    /// <param name="toBeDeletedIds">The row identifiers to delete.</param>
    /// <param name="tableName">The target table.</param>
    /// <param name="databaseName">The owning database.</param>
    public void DeleteFromTable(List<long> toBeDeletedIds, string tableName, string databaseName)
    {
        foreach (long id in toBeDeletedIds)
        {
            _storageEngine.DeleteRow(databaseName, tableName, id);
        }
    }

    /// <summary>
    /// Determines whether a specific row identifier resolves to an existing row.
    /// </summary>
    /// <param name="rowId">The row identifier to validate.</param>
    /// <param name="tableName">The table containing the row.</param>
    /// <param name="databaseName">The owning database.</param>
    /// <returns><see langword="true"/> when the row exists; otherwise <see langword="false"/>.</returns>
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

    /// <summary>
    /// Selects rows from a table, optionally projecting a subset of columns.
    /// </summary>
    /// <param name="ids">The row identifiers to fetch, or <see langword="null"/> for a full scan.</param>
    /// <param name="requestedColumns">The requested column names.</param>
    /// <param name="tableName">The source table.</param>
    /// <param name="databaseName">The owning database.</param>
    /// <returns>The selected rows keyed by row identifier.</returns>
    public Dictionary<long, Dictionary<string, dynamic>> SelectFromTable(List<long>? ids, List<string> requestedColumns,
        string tableName, string databaseName)
    {
        return GetTableContents(ids, tableName, databaseName, NormalizeSelectedColumns(requestedColumns));
    }

    /// <summary>
    /// Fetches all records from a table, decrypting them from bytes back to typed dictionaries.
    /// </summary>
    public Dictionary<long, Dictionary<string, dynamic>> GetTableContents(string tableName, string databaseName)
    {
        return GetTableContents(null, tableName, databaseName, null);
    }

    /// <summary>
    /// Fetches specific records (via IDs/RowIds) from a table, decrypting them from bytes back to typed dictionaries.
    /// Used heavily by Index-driven querying (B+Tree).
    /// </summary>
    public Dictionary<long, Dictionary<string, dynamic>> GetTableContents(List<long>? rowIds, string tableName, string databaseName)
    {
        return GetTableContents(rowIds, tableName, databaseName, null);
    }

    /// <summary>
    /// Fetches table contents and optionally projects selected columns.
    /// </summary>
    /// <param name="rowIds">The row identifiers to fetch, or <see langword="null"/> for a full scan.</param>
    /// <param name="tableName">The source table.</param>
    /// <param name="databaseName">The owning database.</param>
    /// <param name="selectedColumns">The projected columns, or <see langword="null"/> for all columns.</param>
    /// <returns>The materialized rows keyed by row identifier.</returns>
    public Dictionary<long, Dictionary<string, dynamic>> GetTableContents(List<long>? rowIds, string tableName, string databaseName,
        HashSet<string>? selectedColumns)
    {
        return rowIds != null
            ? ReadRowsById(rowIds, tableName, databaseName, selectedColumns)
            : ReadAllRows(tableName, databaseName, selectedColumns);
    }

    /// <summary>
    /// Drops a database and all of its table data.
    /// </summary>
    /// <param name="databaseName">The database to drop.</param>
    public void DropDatabase(string databaseName)
    {
        _storageEngine.DropDatabase(databaseName);
    }

    /// <summary>
    /// Compacts a table by rewriting surviving rows and returning their new identifiers.
    /// </summary>
    /// <param name="tableName">The table to compact.</param>
    /// <param name="databaseName">The owning database.</param>
    /// <returns>The rewritten rows paired with their new identifiers.</returns>
    public List<(long NewRowId, byte[] RawRow)> CompactTable(string tableName, string databaseName)
    {
        return _storageEngine.CompactTable(databaseName, tableName);
    }

    /// <summary>
    /// Serializes a row batch before insertion.
    /// </summary>
    private static List<byte[]> SerializeRows(List<Dictionary<string, dynamic>> rows, string tableName, string databaseName)
    {
        var serializedRows = new List<byte[]>(rows.Count);
        foreach (var row in rows)
        {
            serializedRows.Add(RowSerializer.Serialize(databaseName, tableName, row));
        }

        return serializedRows;
    }

    /// <summary>
    /// Normalizes requested columns by stripping optional table qualifiers.
    /// </summary>
    private static HashSet<string>? NormalizeSelectedColumns(List<string> requestedColumns)
    {
        if (requestedColumns.Count == 0)
        {
            return null;
        }

        return [.. requestedColumns.Select(c => c.Contains('.') ? c[(c.LastIndexOf('.') + 1)..] : c)];
    }

    /// <summary>
    /// Reads explicit row identifiers from storage.
    /// </summary>
    private Dictionary<long, Dictionary<string, dynamic>> ReadRowsById(List<long> rowIds, string tableName, string databaseName,
        HashSet<string>? selectedColumns)
    {
        var parsedTableData = new Dictionary<long, Dictionary<string, dynamic>>();

        if (rowIds.Count == 0)
        {
            return parsedTableData;
        }

        foreach (long rowId in rowIds)
        {
            byte[] rawRow = _storageEngine.ReadRow(databaseName, tableName, rowId);
            parsedTableData[rowId] = RowSerializer.Deserialize(databaseName, tableName, rawRow, selectedColumns);
        }

        return parsedTableData;
    }

    /// <summary>
    /// Reads every live row from the target table.
    /// </summary>
    private Dictionary<long, Dictionary<string, dynamic>> ReadAllRows(string tableName, string databaseName,
        HashSet<string>? selectedColumns)
    {
        var parsedTableData = new Dictionary<long, Dictionary<string, dynamic>>();

        foreach (var rowTuple in _storageEngine.ReadAllRows(databaseName, tableName))
        {
            parsedTableData[rowTuple.RowId] = RowSerializer.Deserialize(databaseName, tableName, rowTuple.RawRow, selectedColumns);
        }

        return parsedTableData;
    }
}
