using DataVo.Core.Exceptions;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.StorageEngine.Serialization;
using DataVo.Core.StorageEngine.Backends;
using DataVo.Core.StorageEngine.Backends.Abstractions;
using DataVo.Core.StorageEngine.Memory;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Diagnostics;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.Transactions;
using DataVo.Core.MVCC;

namespace DataVo.Core.StorageEngine;

/// <summary>
/// The primary Data Access Object for the engine. Replaces the old MongoDB 'DbContext'.
/// All SQL operations route through this context to interact with the underlying byte storage.
/// </summary>
/// <example>
/// <code>
/// StorageContext.Initialize(new DataVoConfig { StorageMode = StorageMode.InMemory });
/// StorageContext context = StorageContext.Instance;
/// context.InsertOneIntoTable(new Dictionary&lt;string, object?&gt; { ["Id"] = 1 }, "Users", "DemoDb");
/// </code>
/// </example>
public class StorageContext(DataVoConfig config)
{
    /// <summary>
    /// Gets the configuration that created this context.
    /// </summary>
    public DataVoConfig Config { get; } = config;

    private readonly IStorageEngine _storageEngine = ResolveStorageEngine(config);
    private EngineCatalog? _runtimeCatalog;
    private string _serializerSchemaScopeKey = "storage-context";
    private static StorageContext? _instance;

    /// <summary>
    /// Gets the resolved storage backend metadata for this context.
    /// </summary>
    public IStorageBackend? Backend => _storageEngine as IStorageBackend;

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

    internal void AttachRuntimeCatalog(EngineCatalog catalog, Guid engineId)
    {
        _runtimeCatalog = catalog;
        _serializerSchemaScopeKey = engineId.ToString("N");
    }

    private static IStorageEngine ResolveStorageEngine(DataVoConfig config)
    {
        return config.StorageMode switch
        {
            StorageMode.InMemory => new InMemoryStorageBackend(),
            StorageMode.Disk => new DiskStorageBackend(config.DiskStoragePath ?? "./datavo_data"),
            StorageMode.Wasm => new WasmStorageBackend(config.WasmStorageEngine),
            StorageMode.Custom => config.CustomStorageEngine ?? throw new ArgumentNullException(nameof(config.CustomStorageEngine), "Custom Storage Mode requires a CustomStorageEngine instance."),
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    /// <summary>
    /// Creates a logical table container in the active storage engine.
    /// </summary>
    /// <param name="tableName">The table to create.</param>
    /// <param name="databaseName">The owning database.</param>
    public void CreateTable(string tableName, string databaseName)
    {
        if (_storageEngine is DiskStorageBackend diskBackend)
        {
            diskBackend.CreateTable(databaseName, tableName);
        }
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
    public long InsertOneIntoTable(Dictionary<string, object?> row, string tableName, string databaseName)
    {
        byte[] serializedData = RowSerializer.Serialize(
            databaseName,
            tableName,
            row,
            ResolveCatalog(),
            ResolveSerializerSchemaScopeKey());
        return _storageEngine.InsertRow(databaseName, tableName, serializedData);
    }

    /// <summary>
    /// Serializes and inserts multiple rows into a table.
    /// </summary>
    /// <param name="rows">The rows to insert.</param>
    /// <param name="tableName">The target table.</param>
    /// <param name="databaseName">The owning database.</param>
    /// <returns>The assigned row identifiers.</returns>
    public List<long> InsertIntoTable(List<Dictionary<string, object?>> rows, string tableName, string databaseName)
    {
        if (rows.Count == 0) return [];

        return _storageEngine.InsertRows(databaseName, tableName, SerializeRows(rows, tableName, databaseName));
    }

    /// <summary>
    /// Serializes and inserts typed storage rows without materializing dictionaries.
    /// </summary>
    internal List<long> InsertTypedRows(IReadOnlyList<StoredRow> rows, string tableName, string databaseName)
    {
        if (rows.Count == 0) return [];

        IReadOnlyList<Column> columns = ResolveCatalog().GetTableColumns(tableName, databaseName);
        var serializedRows = new List<byte[]>(rows.Count);
        foreach (StoredRow row in rows)
        {
            serializedRows.Add(RowSerializer.SerializeCells(columns, row.AsView().Cells));
        }

        return _storageEngine.InsertRows(databaseName, tableName, serializedRows);
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
            var bytes = _storageEngine.ReadRow(databaseName, tableName, rowId);
            return bytes.Length > 0;
        }
        catch (RowDeletedException)
        {
            // Row was tombstoned — logically deleted but physically present.
            return false;
        }
        catch (RowNotFoundException)
        {
            // Row coordinate is outside current table bounds.
            return false;
        }
        catch (FileNotFoundException)
        {
            // Data file does not exist yet (table is empty or never had an insert).
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
    public Dictionary<long, Dictionary<string, object?>> SelectFromTable(List<long>? ids, List<string> requestedColumns,
        string tableName, string databaseName)
    {
        return GetTableContents(ids, tableName, databaseName, NormalizeSelectedColumns(requestedColumns));
    }

    /// <summary>
    /// Fetches all records from a table, decrypting them from bytes back to typed dictionaries.
    /// </summary>
    public Dictionary<long, Dictionary<string, object?>> GetTableContents(string tableName, string databaseName)
    {
        return GetTableContents(null, tableName, databaseName, null);
    }

    /// <summary>
    /// Fetches specific records (via IDs/RowIds) from a table, decrypting them from bytes back to typed dictionaries.
    /// Used heavily by Index-driven querying (B+Tree).
    /// </summary>
    public Dictionary<long, Dictionary<string, object?>> GetTableContents(List<long>? rowIds, string tableName, string databaseName)
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
    public Dictionary<long, Dictionary<string, object?>> GetTableContents(List<long>? rowIds, string tableName, string databaseName,
        HashSet<string>? selectedColumns)
    {
        Dictionary<long, Dictionary<string, object?>> rows = rowIds != null
            ? ReadRowsById(rowIds, tableName, databaseName, selectedColumns)
            : ReadAllRows(tableName, databaseName, selectedColumns);

        return ApplyMvccVisibilityFilter(rows, tableName, databaseName);
    }

    /// <summary>
    /// Fetches all records from a table as typed storage rows, bypassing dictionary materialization.
    /// </summary>
    internal Dictionary<long, StoredRow> GetTypedTableContents(string tableName, string databaseName)
    {
        return GetTypedTableContents(null, tableName, databaseName);
    }

    /// <summary>
    /// Fetches specific records from a table as typed storage rows, bypassing dictionary materialization.
    /// </summary>
    internal Dictionary<long, StoredRow> GetTypedTableContents(List<long>? rowIds, string tableName, string databaseName)
    {
        Dictionary<long, StoredRow> rows = rowIds != null
            ? ReadTypedRowsById(rowIds, tableName, databaseName)
            : ReadAllTypedRows(tableName, databaseName);

        return ApplyTypedMvccVisibilityFilter(rows, tableName, databaseName);
    }

    private static Dictionary<long, Dictionary<string, object?>> ApplyMvccVisibilityFilter(
        Dictionary<long, Dictionary<string, object?>> rows,
        string tableName,
        string databaseName)
    {
        TransactionSnapshot? snapshot = MvccExecutionScope.CurrentSnapshot;
        if (snapshot == null || rows.Count == 0)
        {
            return rows;
        }

        DataVoEngine engine = DataVoEngine.Current();
        List<long> toRemove = [];

        foreach (long rowId in rows.Keys)
        {
            RowVersion version = MvccCoordinator.EnsureRowVersionExists(engine, databaseName, tableName, rowId);
            if (!SnapshotVisibilityEvaluator.IsVersionVisible(version, snapshot))
            {
                toRemove.Add(rowId);
            }
        }

        if (toRemove.Count == 0)
        {
            return rows;
        }

        foreach (long rowId in toRemove)
        {
            rows.Remove(rowId);
        }

        return rows;
    }

    private static Dictionary<long, StoredRow> ApplyTypedMvccVisibilityFilter(
        Dictionary<long, StoredRow> rows,
        string tableName,
        string databaseName)
    {
        TransactionSnapshot? snapshot = MvccExecutionScope.CurrentSnapshot;
        if (snapshot == null || rows.Count == 0)
        {
            return rows;
        }

        DataVoEngine engine = DataVoEngine.Current();
        List<long> toRemove = [];

        foreach (long rowId in rows.Keys)
        {
            RowVersion version = MvccCoordinator.EnsureRowVersionExists(engine, databaseName, tableName, rowId);
            if (!SnapshotVisibilityEvaluator.IsVersionVisible(version, snapshot))
            {
                toRemove.Add(rowId);
            }
        }

        if (toRemove.Count == 0)
        {
            return rows;
        }

        foreach (long rowId in toRemove)
        {
            rows.Remove(rowId);
        }

        return rows;
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
    /// <param name="allowIndexedCompaction">
    /// Set to <see langword="true"/> only when the caller also rebuilds all table indexes from the returned row mapping.
    /// </param>
    /// <returns>The rewritten rows paired with their new identifiers.</returns>
    public List<(long NewRowId, byte[] RawRow)> CompactTable(string tableName, string databaseName, bool allowIndexedCompaction = false)
    {
        if (!allowIndexedCompaction)
        {
            EngineCatalog catalog = ResolveCatalog();
            List<IndexFile> tableIndexes = catalog.GetTableIndexes(tableName, databaseName);
            if (tableIndexes.Count > 0)
            {
                throw new InvalidOperationException(
                    $"Compacting indexed table '{databaseName}.{tableName}' requires index rebuild. Use VACUUM or pass allowIndexedCompaction=true and rebuild indexes.");
            }
        }

        return _storageEngine.CompactTable(databaseName, tableName);
    }

    internal InMemoryStorageSnapshot CreateInMemorySnapshot()
    {
        if (_storageEngine is IInMemoryStorageSnapshotProvider snapshotProvider)
        {
            return snapshotProvider.CreateSnapshot();
        }

        throw new NotSupportedException("Storage snapshots are only supported by the in-memory storage backend.");
    }

    internal void RestoreInMemorySnapshot(InMemoryStorageSnapshot snapshot)
    {
        if (_storageEngine is IInMemoryStorageSnapshotProvider snapshotProvider)
        {
            snapshotProvider.RestoreSnapshot(snapshot);
            return;
        }

        throw new NotSupportedException("Storage snapshots are only supported by the in-memory storage backend.");
    }

    /// <summary>
    /// Serializes a row batch before insertion.
    /// </summary>
    private List<byte[]> SerializeRows(List<Dictionary<string, object?>> rows, string tableName, string databaseName)
    {
        EngineCatalog catalog = ResolveCatalog();
        string schemaScopeKey = ResolveSerializerSchemaScopeKey();
        var serializedRows = new List<byte[]>(rows.Count);
        foreach (var row in rows)
        {
            serializedRows.Add(RowSerializer.Serialize(databaseName, tableName, row, catalog, schemaScopeKey));
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
    private Dictionary<long, Dictionary<string, object?>> ReadRowsById(List<long> rowIds, string tableName, string databaseName,
        HashSet<string>? selectedColumns)
    {
        var parsedTableData = new Dictionary<long, Dictionary<string, object?>>();

        if (rowIds.Count == 0)
        {
            return parsedTableData;
        }

        foreach (long rowId in rowIds)
        {
            byte[] rawRow = _storageEngine.ReadRow(databaseName, tableName, rowId);
            parsedTableData[rowId] = RowSerializer.Deserialize(
                databaseName,
                tableName,
                rawRow,
                selectedColumns,
                ResolveCatalog(),
                ResolveSerializerSchemaScopeKey());
        }

        RuntimeQueryDiagnosticsScope.RecordTableRead(tableName, parsedTableData.Count);

        return parsedTableData;
    }

    /// <summary>
    /// Reads explicit row identifiers from storage as typed rows.
    /// </summary>
    private Dictionary<long, StoredRow> ReadTypedRowsById(List<long> rowIds, string tableName, string databaseName)
    {
        var parsedTableData = new Dictionary<long, StoredRow>();

        if (rowIds.Count == 0)
        {
            return parsedTableData;
        }

        IReadOnlyList<Column> columns = ResolveCatalog().GetTableColumns(tableName, databaseName);
        ReactiveRowSchema schema = BuildReactiveSchema(columns);

        foreach (long rowId in rowIds)
        {
            byte[] rawRow = _storageEngine.ReadRow(databaseName, tableName, rowId);
            parsedTableData[rowId] = new StoredRow(schema, RowSerializer.DeserializeCells(rawRow, columns));
        }

        RuntimeQueryDiagnosticsScope.RecordTableRead(tableName, parsedTableData.Count);

        return parsedTableData;
    }

    /// <summary>
    /// Reads every live row from the target table.
    /// </summary>
    private Dictionary<long, Dictionary<string, object?>> ReadAllRows(string tableName, string databaseName,
        HashSet<string>? selectedColumns)
    {
        var parsedTableData = new Dictionary<long, Dictionary<string, object?>>();

        foreach (var rowTuple in _storageEngine.ReadAllRows(databaseName, tableName))
        {
            parsedTableData[rowTuple.RowId] = RowSerializer.Deserialize(
                databaseName,
                tableName,
                rowTuple.RawRow,
                selectedColumns,
                ResolveCatalog(),
                ResolveSerializerSchemaScopeKey());
        }

        RuntimeQueryDiagnosticsScope.RecordTableScan(tableName, parsedTableData.Count);

        return parsedTableData;
    }

    /// <summary>
    /// Reads every live row from the target table as typed rows.
    /// </summary>
    private Dictionary<long, StoredRow> ReadAllTypedRows(string tableName, string databaseName)
    {
        var parsedTableData = new Dictionary<long, StoredRow>();
        IReadOnlyList<Column> columns = ResolveCatalog().GetTableColumns(tableName, databaseName);
        ReactiveRowSchema schema = BuildReactiveSchema(columns);

        foreach (var rowTuple in _storageEngine.ReadAllRows(databaseName, tableName))
        {
            parsedTableData[rowTuple.RowId] = new StoredRow(
                schema,
                RowSerializer.DeserializeCells(rowTuple.RawRow, columns));
        }

        RuntimeQueryDiagnosticsScope.RecordTableScan(tableName, parsedTableData.Count);

        return parsedTableData;
    }

    private static ReactiveRowSchema BuildReactiveSchema(IReadOnlyList<Column> columns)
    {
        var names = new string[columns.Count];
        for (int i = 0; i < columns.Count; i++)
        {
            names[i] = columns[i].Name;
        }

        return new ReactiveRowSchema(names);
    }

    private EngineCatalog ResolveCatalog()
    {
        return _runtimeCatalog ?? DataVoEngine.Current().Catalog;
    }

    private string ResolveSerializerSchemaScopeKey()
    {
        if (_runtimeCatalog != null)
        {
            return _serializerSchemaScopeKey;
        }

        return DataVoEngine.Current().Id.ToString("N");
    }
}
