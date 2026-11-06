using DataVo.Core.Exceptions;
using DataVo.Core.Compat;
using DataVo.Core.CompiledQueries;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.StorageEngine.Serialization;
using DataVo.Core.StorageEngine.Backends;
using DataVo.Core.StorageEngine.Backends.Abstractions;
using DataVo.Core.StorageEngine.Lsm;
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
public class StorageContext(DataVoConfig config) : IDisposable
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
        if (_storageEngine is LsmStorageBackend lsm)
        {
            lsm.ClusteredKeys = ResolveClusteredKey;
        }
    }

    // Clustered key layouts per table, valid for the schema version they were built at.
    private readonly System.Collections.Concurrent.ConcurrentDictionary<(string Database, string Table), (int Version, ClusteredKeyLayout? Layout)> _clusteredLayouts = new();

    /// <summary>
    /// The clustered key layout of a table (see <see cref="Table.Clustered"/>), or <see langword="null"/>
    /// when its rows have engine-assigned ids. Asked on every insert and primary-key lookup, so it is
    /// cached per table and rebuilt only when the table's schema version changes.
    /// </summary>
    internal ClusteredKeyLayout? ResolveClusteredKey(string databaseName, string tableName)
    {
        EngineCatalog catalog = ResolveCatalog();
        int version = catalog.GetTableSchemaVersion(tableName, databaseName);
        if (_clusteredLayouts.TryGetValue((databaseName, tableName), out (int Version, ClusteredKeyLayout? Layout) cached) && cached.Version == version)
        {
            return cached.Layout;
        }

        ClusteredKeyLayout? layout = null;
        if (catalog.IsTableClustered(tableName, databaseName))
        {
            List<string> primaryKeys = catalog.GetTablePrimaryKeys(tableName, databaseName);
            List<Column> columns = catalog.GetTableColumns(tableName, databaseName);
            int ordinal = primaryKeys.Count == 1
                ? columns.FindIndex(column => string.Equals(column.Name, primaryKeys[0], StringComparison.OrdinalIgnoreCase))
                : -1;
            if (ordinal < 0)
            {
                throw new CatalogException($"Clustered table {tableName} has no single primary-key column.");
            }

            layout = new ClusteredKeyLayout(columns, ordinal, $"_PK_{tableName}");
        }

        _clusteredLayouts[(databaseName, tableName)] = (version, layout);
        return layout;
    }

    private static IStorageEngine ResolveStorageEngine(DataVoConfig config)
    {
        PortableStoragePolicy.ThrowIfUnsupported(config.StorageMode);

        return config.StorageMode switch
        {
            StorageMode.InMemory => new InMemoryStorageBackend(),
            StorageMode.Disk => new DiskStorageBackend(config.ResolveDataDirectory()!, config.SyncDiskWrites, config.IoSchedulerMode),
            StorageMode.Lsm => new LsmStorageBackend(
                config.ResolveDataDirectory()!,
                config.LsmStrictFsync ? LsmWalDurabilityMode.StrictFsync : LsmWalDurabilityMode.RelaxedOsBuffer,
                config.LsmReadMode),
            StorageMode.Wasm => new WasmStorageBackend(config.WasmStorageEngine),
            StorageMode.Custom => config.CustomStorageEngine ?? throw new ArgumentNullException(nameof(config.CustomStorageEngine), "Custom Storage Mode requires a CustomStorageEngine instance."),
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    /// <summary>
    /// Forces all buffered disk writes to the physical device, when the active backend is disk-backed.
    /// Used by the WAL checkpointer to make data-file writes durable before advancing the checkpoint LSN.
    /// </summary>
    internal IWriteBatchScope BeginWriteBatch() =>
        _storageEngine is LsmStorageBackend lsm ? lsm.BeginWriteBatch() : NoWriteBatch.Instance;

    private sealed class NoWriteBatch : IWriteBatchScope
    {
        public static readonly NoWriteBatch Instance = new();

        public void Complete()
        {
        }

        public void Dispose()
        {
        }
    }

    internal void FlushBackendToDisk()
    {
        if (_storageEngine is DiskStorageBackend diskBackend)
        {
            diskBackend.FlushToDisk();
        }
    }

    /// <summary>
    /// Releases resources owned by the resolved storage backend.
    /// </summary>
    public void Dispose()
    {
        if (_storageEngine is IDisposable disposable)
        {
            disposable.Dispose();
        }
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
    public RowKey InsertOneIntoTable(Dictionary<string, object?> row, string tableName, string databaseName)
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
    /// Appends an already-serialized row payload to a table, skipping dictionary materialization and
    /// re-serialization. Used by the zero-allocation update path, which produces the new-row bytes by
    /// byte-patching the previous row in place.
    /// </summary>
    /// <param name="rowBytes">The serialized row payload, already in the on-disk wire format.</param>
    /// <param name="tableName">The target table.</param>
    /// <param name="databaseName">The owning database.</param>
    /// <returns>The assigned row identifier.</returns>
    internal RowKey InsertSerializedRow(byte[] rowBytes, string tableName, string databaseName)
    {
        return _storageEngine.InsertRow(databaseName, tableName, rowBytes);
    }

    internal bool TryPatchFixedWidthRow(
        string tableName,
        string databaseName,
        RowKey rowId,
        IReadOnlyList<Column> columns,
        ReadOnlySpan<int> ordinals,
        ReadOnlySpan<DataVoFixedWidthValue> values)
    {
        return _storageEngine is IFixedWidthPatchStorageEngine patchStorage
            && patchStorage.TryPatchFixedWidthRow(databaseName, tableName, rowId, columns, ordinals, values);
    }

    internal int TryPatchFixedWidthRows(
        string tableName,
        string databaseName,
        IReadOnlyList<Column> columns,
        ReadOnlySpan<int> ordinals,
        IReadOnlyList<FixedWidthPatchOperation> operations)
    {
        return _storageEngine is IFixedWidthPatchStorageEngine patchStorage
            ? patchStorage.TryPatchFixedWidthRows(databaseName, tableName, columns, ordinals, operations)
            : 0;
    }

    /// <summary>
    /// Serializes and inserts multiple rows into a table.
    /// </summary>
    /// <param name="rows">The rows to insert.</param>
    /// <param name="tableName">The target table.</param>
    /// <param name="databaseName">The owning database.</param>
    /// <returns>The assigned row identifiers.</returns>
    public List<RowKey> InsertIntoTable(List<Dictionary<string, object?>> rows, string tableName, string databaseName)
    {
        if (rows.Count == 0) return [];

        return (_storageEngine.InsertRows(databaseName, tableName, SerializeRows(rows, tableName, databaseName)));
    }

    /// <summary>
    /// Serializes and inserts typed storage rows without materializing dictionaries.
    /// </summary>
    internal List<RowKey> InsertTypedRows(IReadOnlyList<StoredRow> rows, string tableName, string databaseName)
    {
        if (rows.Count == 0) return [];

        if (_storageEngine is ITypedRowStorageEngine typedStorage)
        {
            return RowKeyConversions.ToRowKeys(typedStorage.InsertTypedRows(databaseName, tableName, rows));
        }

        IReadOnlyList<Column> columns = ResolveCatalog().GetTableColumns(tableName, databaseName);
        var serializedRows = new List<byte[]>(rows.Count);
        foreach (StoredRow row in rows)
        {
            serializedRows.Add(RowSerializer.SerializeCells(columns, row.AsView().Cells));
        }

        return (_storageEngine.InsertRows(databaseName, tableName, serializedRows));
    }

    /// <summary>
    /// Scratch list handed to <see cref="IStorageEngine.InsertRows"/>, which only iterates it (no
    /// backend retains the list itself — the LSM/disk engines retain or write the row buffers).
    /// Thread-static so concurrent sessions never share it; cleared after use so it doesn't pin
    /// row buffers between batches.
    /// </summary>
    [ThreadStatic]
    private static List<byte[]>? _typedInsertSerializeScratch;

    /// <summary>
    /// Serializes and inserts full typed cell rows for serialize-through backends without
    /// materializing per-row <see cref="StoredRow"/> wrappers. Wire-identical to
    /// <see cref="InsertTypedRows(IReadOnlyList{StoredRow}, string, string)"/>. Must not be used
    /// when <see cref="RetainsTypedRowBuffers"/> is <see langword="true"/> — in-memory typed storage
    /// needs the row wrappers themselves.
    /// </summary>
    internal List<RowKey> InsertTypedCellRows(
        IReadOnlyList<Column> columns,
        IReadOnlyList<CellValue[]> rows,
        string tableName,
        string databaseName)
    {
        if (rows.Count == 0) return [];

        if (_storageEngine is ITypedRowStorageEngine)
        {
            throw new InvalidOperationException(
                "InsertTypedCellRows is only valid for serialize-through backends; typed storage retains row buffers.");
        }

        // A clustered table's rows go straight to their stored form: the key comes from the cells and the
        // row is serialized once, without it (instead of serialized whole, read back, and copied).
        if (_storageEngine is LsmStorageBackend lsm && ResolveClusteredKey(databaseName, tableName) is { } layout)
        {
            var keys = new List<RowKey>(rows.Count);
            var stored = new List<byte[]>(rows.Count);
            for (int i = 0; i < rows.Count; i++)
            {
                keys.Add(layout.KeyOf(rows[i], tableName));
                stored.Add(RowSerializer.SerializeCells(columns, rows[i], layout.Ordinal));
            }

            return lsm.InsertStoredRows(databaseName, tableName, keys, stored);
        }

        List<byte[]> serializedRows = _typedInsertSerializeScratch ??= [];
        try
        {
            if (serializedRows.Capacity < rows.Count)
            {
                serializedRows.Capacity = rows.Count;
            }
            for (int i = 0; i < rows.Count; i++)
            {
                serializedRows.Add(RowSerializer.SerializeCells(columns, rows[i]));
            }

            return (_storageEngine.InsertRows(databaseName, tableName, serializedRows));
        }
        finally
        {
            serializedRows.Clear();
        }
    }

    /// <summary>
    /// Serializes and inserts a single typed storage row via the single-row backend path, without
    /// per-row List churn or a redundant catalog column lookup. Wire-identical to the batch path.
    /// </summary>
    internal RowKey InsertTypedRow(StoredRow row, IReadOnlyList<Column> columns, string tableName, string databaseName)
    {
        if (_storageEngine is ITypedRowStorageEngine typedStorage)
        {
            return typedStorage.InsertTypedRow(databaseName, tableName, row);
        }

        byte[] serialized = RowSerializer.SerializeCells(columns, row.AsView().Cells);
        return _storageEngine.InsertRow(databaseName, tableName, serialized);
    }

    /// <summary>
    /// Deletes the specified row identifiers from a table.
    /// </summary>
    /// <param name="toBeDeletedIds">The row identifiers to delete.</param>
    /// <param name="tableName">The target table.</param>
    /// <param name="databaseName">The owning database.</param>
    public void DeleteFromTable(List<RowKey> toBeDeletedIds, string tableName, string databaseName)
    {
        foreach (RowKey id in toBeDeletedIds)
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
    public bool TableContainsRow(RowKey rowId, string tableName, string databaseName)
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
    public Dictionary<RowKey, Dictionary<string, object?>> SelectFromTable(List<RowKey>? ids, List<string> requestedColumns,
        string tableName, string databaseName)
    {
        return GetTableContents(ids, tableName, databaseName, NormalizeSelectedColumns(requestedColumns));
    }

    /// <summary>
    /// Fetches all records from a table, decrypting them from bytes back to typed dictionaries.
    /// </summary>
    public Dictionary<RowKey, Dictionary<string, object?>> GetTableContents(string tableName, string databaseName)
    {
        return GetTableContents(null, tableName, databaseName, null);
    }

    /// <summary>
    /// Fetches specific records (via IDs/RowIds) from a table, decrypting them from bytes back to typed dictionaries.
    /// Used heavily by Index-driven querying (B+Tree).
    /// </summary>
    public Dictionary<RowKey, Dictionary<string, object?>> GetTableContents(List<RowKey>? rowIds, string tableName, string databaseName)
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
    public Dictionary<RowKey, Dictionary<string, object?>> GetTableContents(List<RowKey>? rowIds, string tableName, string databaseName,
        HashSet<string>? selectedColumns)
    {
        Dictionary<RowKey, Dictionary<string, object?>> rows = rowIds != null
            ? ReadRowsById(rowIds, tableName, databaseName, selectedColumns)
            : ReadAllRows(tableName, databaseName, selectedColumns);

        return ApplyMvccVisibilityFilter(rows, tableName, databaseName);
    }

    /// <summary>
    /// Fetches all records from a table as typed storage rows, bypassing dictionary materialization.
    /// </summary>
    internal Dictionary<RowKey, StoredRow> GetTypedTableContents(string tableName, string databaseName)
    {
        return GetTypedTableContents(null, tableName, databaseName);
    }

    /// <summary>
    /// Returns the stored serialized bytes for one row (in-memory: the stored reference, no copy), or
    /// <c>null</c> when the row was deleted or does not exist.
    /// </summary>
    internal byte[]? TryReadRowBytes(string tableName, string databaseName, RowKey rowId)
    {
        if (_storageEngine is ITypedRowStorageEngine typedStorage
            && typedStorage.TryReadTypedRow(databaseName, tableName, RowKeyConversions.ToRowId(rowId), out _))
        {
            return null;
        }

        try
        {
            return _storageEngine.ReadRow(databaseName, tableName, rowId);
        }
        catch (RowDeletedException)
        {
            return null;
        }
        catch (RowNotFoundException)
        {
            return null;
        }
    }

    internal bool TryReadStoredRow(string tableName, string databaseName, RowKey rowId, out StoredRow? row)
    {
        if (_storageEngine is ITypedRowStorageEngine typedStorage
            && typedStorage.TryReadTypedRow(databaseName, tableName, RowKeyConversions.ToRowId(rowId), out row))
        {
            return true;
        }

        row = null;
        byte[]? bytes = TryReadRowBytes(tableName, databaseName, rowId);
        if (bytes is null)
        {
            return false;
        }

        IReadOnlyList<Column> columns = ResolveCatalog().GetTableColumns(tableName, databaseName);
        row = StoredRow.FromOwnedCells(BuildReactiveSchema(columns), RowSerializer.DeserializeCells(bytes, columns));
        return true;
    }

    /// <summary>
    /// Whether the active backend retains typed row cell buffers as its storage representation
    /// (in-memory typed storage) rather than serializing them transiently during the insert call.
    /// </summary>
    internal bool RetainsTypedRowBuffers => _storageEngine is ITypedRowStorageEngine;

    /// <summary>
    /// Cheap row-count estimate for query planning. Returns false when the backend cannot answer
    /// without reading the table.
    /// </summary>
    internal bool TryEstimateRowCount(string tableName, string databaseName, out long count)
    {
        if (_storageEngine is IRowCountEstimator estimator)
        {
            count = estimator.EstimateRowCount(databaseName, tableName);
            return true;
        }

        count = 0;
        return false;
    }

    /// <summary>The LSM engine when it is the backend (test hooks and diagnostics); otherwise null.</summary>
    internal Lsm.LsmStorageEngine? LsmEngine => (_storageEngine as Backends.LsmStorageBackend)?.Engine;

    /// <summary>Persisted index storage when the backend is the LSM engine; otherwise null.</summary>
    internal ILsmIndexStore? LsmIndexStore => (_storageEngine as Backends.LsmStorageBackend)?.IndexStore;

    internal bool IsTableEmpty(string tableName, string databaseName)
    {
        if (_storageEngine is ITypedRowStorageEngine typedStorage)
        {
            return !typedStorage.HasAnyRows(databaseName, tableName);
        }

        if (_storageEngine is IRowExistenceProbe probe)
        {
            return !probe.HasAnyRows(databaseName, tableName);
        }

        foreach ((_, byte[] _) in _storageEngine.ReadAllRows(databaseName, tableName))
        {
            return false;
        }

        return true;
    }

    /// <summary>
    /// Per-row MVCC visibility, identical in effect to <see cref="ApplyTypedMvccVisibilityFilter"/>: visible when
    /// there is no active snapshot, otherwise gated by the row version's snapshot visibility.
    /// </summary>
    internal bool IsRowVisible(string tableName, string databaseName, RowKey rowId)
    {
        TransactionSnapshot? snapshot = MvccExecutionScope.CurrentSnapshot;
        if (snapshot == null)
        {
            return true;
        }

        DataVoEngine engine = DataVoEngine.Current();
        RowVersion version = MvccCoordinator.EnsureRowVersionExists(engine, databaseName, tableName, rowId);
        return SnapshotVisibilityEvaluator.IsVersionVisible(version, snapshot);
    }

    /// <summary>
    /// Fetches specific records from a table as typed storage rows, bypassing dictionary materialization.
    /// </summary>
    internal Dictionary<RowKey, StoredRow> GetTypedTableContents(List<RowKey>? rowIds, string tableName, string databaseName)
    {
        Dictionary<RowKey, StoredRow> rows = rowIds != null
            ? ReadTypedRowsById(rowIds, tableName, databaseName)
            : ReadAllTypedRows(tableName, databaseName);

        return ApplyTypedMvccVisibilityFilter(rows, tableName, databaseName);
    }

    /// <summary>Visits one row of <see cref="ScanTableRows{TState}"/>: serialized bytes, or a typed row for typed backends.</summary>
    internal delegate bool RawRowVisitor<TState>(ref TState state, RowKey rowId, ReadOnlySpan<byte> rowBytes, StoredRow? typedRow);

    /// <summary>
    /// Streams the live rows of a table with keys in <c>[lower, upper)</c> (null bounds are open), without
    /// building a table-sized collection. The LSM backend seeks to the range and passes rows in stored form
    /// (a clustered table's key cell is NULL there; the row key holds it). Returns false when a scan is not
    /// possible here (an active snapshot or staged writes), so the caller takes the general path.
    /// </summary>
    internal bool ScanTableRows<TState>(string tableName, string databaseName, RowKey? lower, RowKey? upper, ref TState state, RawRowVisitor<TState> visit)
    {
        if (MvccExecutionScope.CurrentSnapshot is not null)
        {
            return false;
        }

        if (LsmEngine is { } lsm)
        {
            if (lsm.HasStagedWrites(databaseName, tableName))
            {
                return false;
            }

            var box = (State: state, Visit: visit);
            lsm.ScanStoredRows(databaseName, tableName, lower, upper, ref box, static (ref (TState State, RawRowVisitor<TState> Visit) b, RowKey rowId, ReadOnlySpan<byte> bytes) =>
                b.Visit(ref b.State, rowId, bytes, null));
            state = box.State;
            return true;
        }

        if (_storageEngine is ITypedRowStorageEngine typedStorage)
        {
            foreach ((RowKey rowId, StoredRow row) in typedStorage.ReadAllTypedRows(databaseName, tableName))
            {
                if (InRange(rowId, lower, upper) && !visit(ref state, rowId, default, row))
                {
                    return true;
                }
            }
        }

        foreach ((RowKey rowId, byte[] rawRow) in _storageEngine.ReadAllRows(databaseName, tableName))
        {
            if (InRange(rowId, lower, upper) && !visit(ref state, rowId, rawRow, null))
            {
                break;
            }
        }

        return true;
    }

    private static bool InRange(RowKey key, RowKey? lower, RowKey? upper) =>
        (lower is not { } from || key >= from) && (upper is not { } to || key < to);

    private static Dictionary<RowKey, Dictionary<string, object?>> ApplyMvccVisibilityFilter(
        Dictionary<RowKey, Dictionary<string, object?>> rows,
        string tableName,
        string databaseName)
    {
        TransactionSnapshot? snapshot = MvccExecutionScope.CurrentSnapshot;
        if (snapshot == null || rows.Count == 0)
        {
            return rows;
        }

        DataVoEngine engine = DataVoEngine.Current();
        List<RowKey> toRemove = [];

        foreach (RowKey rowId in rows.Keys)
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

        foreach (RowKey rowId in toRemove)
        {
            rows.Remove(rowId);
        }

        return rows;
    }

    private static Dictionary<RowKey, StoredRow> ApplyTypedMvccVisibilityFilter(
        Dictionary<RowKey, StoredRow> rows,
        string tableName,
        string databaseName)
    {
        TransactionSnapshot? snapshot = MvccExecutionScope.CurrentSnapshot;
        if (snapshot == null || rows.Count == 0)
        {
            return rows;
        }

        DataVoEngine engine = DataVoEngine.Current();
        List<RowKey> toRemove = [];

        foreach (RowKey rowId in rows.Keys)
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

        foreach (RowKey rowId in toRemove)
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
    public List<(RowKey NewRowId, byte[] RawRow)> CompactTable(string tableName, string databaseName, bool allowIndexedCompaction = false)
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
    private Dictionary<RowKey, Dictionary<string, object?>> ReadRowsById(List<RowKey> rowIds, string tableName, string databaseName,
        HashSet<string>? selectedColumns)
    {
        var parsedTableData = new Dictionary<RowKey, Dictionary<string, object?>>();

        if (rowIds.Count == 0)
        {
            return parsedTableData;
        }

        if (_storageEngine is ITypedRowStorageEngine typedStorage)
        {
            IReadOnlyList<Column> columns = ResolveCatalog().GetTableColumns(tableName, databaseName);
            foreach (RowKey rowId in rowIds)
            {
                if (typedStorage.TryReadTypedRow(databaseName, tableName, RowKeyConversions.ToRowId(rowId), out StoredRow? typedRow)
                    && typedRow is not null)
                {
                    parsedTableData[rowId] = CopyStoredRowToLegacyDictionary(typedRow, columns, selectedColumns);
                    continue;
                }

                byte[] rawRow;
                try
                {
                    rawRow = _storageEngine.ReadRow(databaseName, tableName, rowId);
                }
                catch (RowDeletedException)
                {
                    continue;
                }

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

        foreach (RowKey rowId in rowIds)
        {
            byte[] rawRow;
            try
            {
                rawRow = _storageEngine.ReadRow(databaseName, tableName, rowId);
            }
            catch (RowDeletedException)
            {
                continue;
            }

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
    private Dictionary<RowKey, StoredRow> ReadTypedRowsById(List<RowKey> rowIds, string tableName, string databaseName)
    {
        var parsedTableData = new Dictionary<RowKey, StoredRow>();

        if (rowIds.Count == 0)
        {
            return parsedTableData;
        }

        IReadOnlyList<Column> columns = ResolveCatalog().GetTableColumns(tableName, databaseName);
        ReactiveRowSchema schema = BuildReactiveSchema(columns);

        if (_storageEngine is ITypedRowStorageEngine typedStorage)
        {
            foreach (RowKey rowId in rowIds)
            {
                if (typedStorage.TryReadTypedRow(databaseName, tableName, RowKeyConversions.ToRowId(rowId), out StoredRow? typedRow)
                    && typedRow is not null)
                {
                    parsedTableData[rowId] = typedRow;
                    continue;
                }

                byte[] rawRow;
                try
                {
                    rawRow = _storageEngine.ReadRow(databaseName, tableName, rowId);
                }
                catch (RowDeletedException)
                {
                    continue;
                }

                parsedTableData[rowId] = StoredRow.FromOwnedCells(schema, RowSerializer.DeserializeCells(rawRow, columns));
            }

            RuntimeQueryDiagnosticsScope.RecordTableRead(tableName, parsedTableData.Count);
            return parsedTableData;
        }

        foreach (RowKey rowId in rowIds)
        {
            byte[] rawRow;
            try
            {
                rawRow = _storageEngine.ReadRow(databaseName, tableName, rowId);
            }
            catch (RowDeletedException)
            {
                continue;
            }

            parsedTableData[rowId] = StoredRow.FromOwnedCells(schema, RowSerializer.DeserializeCells(rawRow, columns));
        }

        RuntimeQueryDiagnosticsScope.RecordTableRead(tableName, parsedTableData.Count);

        return parsedTableData;
    }

    /// <summary>
    /// Reads every live row from the target table.
    /// </summary>
    private Dictionary<RowKey, Dictionary<string, object?>> ReadAllRows(string tableName, string databaseName,
        HashSet<string>? selectedColumns)
    {
        var parsedTableData = new Dictionary<RowKey, Dictionary<string, object?>>();

        if (_storageEngine is ITypedRowStorageEngine typedStorage)
        {
            IReadOnlyList<Column> columns = ResolveCatalog().GetTableColumns(tableName, databaseName);
            foreach ((RowKey rowId, StoredRow row) in typedStorage.ReadAllTypedRows(databaseName, tableName))
            {
                parsedTableData[rowId] = CopyStoredRowToLegacyDictionary(row, columns, selectedColumns);
            }
        }

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
    private Dictionary<RowKey, StoredRow> ReadAllTypedRows(string tableName, string databaseName)
    {
        var parsedTableData = new Dictionary<RowKey, StoredRow>();
        IReadOnlyList<Column> columns = ResolveCatalog().GetTableColumns(tableName, databaseName);
        ReactiveRowSchema schema = BuildReactiveSchema(columns);

        if (_storageEngine is ITypedRowStorageEngine typedStorage)
        {
            foreach ((RowKey rowId, StoredRow row) in typedStorage.ReadAllTypedRows(databaseName, tableName))
            {
                parsedTableData[rowId] = row;
            }
        }

        foreach (var rowTuple in _storageEngine.ReadAllRows(databaseName, tableName))
        {
            parsedTableData[rowTuple.RowId] = StoredRow.FromOwnedCells(
                schema,
                RowSerializer.DeserializeCells(rowTuple.RawRow, columns));
        }

        RuntimeQueryDiagnosticsScope.RecordTableScan(tableName, parsedTableData.Count);

        return parsedTableData;
    }

    private static Dictionary<string, object?> CopyStoredRowToLegacyDictionary(
        StoredRow row,
        IReadOnlyList<Column> columns,
        HashSet<string>? selectedColumns)
    {
        StoredRowView view = row.AsView();
        var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < view.Count; i++)
        {
            string column = view.Schema.ColumnAt(i);
            if (selectedColumns is not null && !selectedColumns.Contains(column))
            {
                continue;
            }

            result[column] = ConvertCellForLegacyDictionary(columns[i], view[i]);
        }

        return result;
    }

    internal static object? ConvertCellForLegacyDictionary(Column column, CellValue cell)
    {
        if (cell.IsNull)
        {
            return null;
        }

        if (column.Type.Equals("FLOAT", StringComparison.OrdinalIgnoreCase))
        {
            return (float)cell.AsDouble();
        }

        return cell.ToObject();
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
