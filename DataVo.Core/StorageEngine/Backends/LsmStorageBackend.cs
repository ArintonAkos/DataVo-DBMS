using DataVo.Core.Exceptions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.CompiledQueries;
using DataVo.Core.StorageEngine.Backends.Abstractions;
using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Core.StorageEngine.Backends;

internal sealed class LsmStorageBackend : IStorageBackend, IFixedWidthPatchStorageEngine, IRowExistenceProbe, IRowCountEstimator, DataVo.Core.Indexing.Columnar.IColumnarRowStore, IDisposable
{
    private readonly LsmStorageEngine _inner;

    public LsmStorageBackend(
        string storagePath,
        LsmWalDurabilityMode walDurabilityMode = LsmWalDurabilityMode.StrictFsync,
        Lsm.V2.LsmReadMode readMode = Lsm.V2.LsmReadMode.Auto)
    {
        _inner = new LsmStorageEngine(storagePath, walDurabilityMode, readMode);
    }

    public string BackendKind => "Lsm";

    public bool KeyedRows => true;

    public DataVo.Core.Indexing.Columnar.ColumnarIndex? Columnar(string databaseName, string tableName) => _inner.Columnar(databaseName, tableName);

    public DataVo.Core.Indexing.Columnar.ColumnarIndex? BuildColumnar(string databaseName, string tableName, Serialization.RowSerializer.StorageColumnType[] tableKinds, int[] ordinals, int schemaVersion, int keyOrdinal) =>
        _inner.BuildColumnar(databaseName, tableName, tableKinds, ordinals, schemaVersion, keyOrdinal);

    public void DropColumnar(string databaseName, string tableName) => _inner.DropColumnar(databaseName, tableName);

    /// <summary>The underlying engine (test hooks and diagnostics).</summary>
    internal LsmStorageEngine Engine => _inner;

    /// <summary>The engine's persisted index storage.</summary>
    internal ILsmIndexStore IndexStore => _inner;

    /// <summary>
    /// Resolves a table's clustered key layout, or <see langword="null"/> for a table with engine-assigned
    /// row ids. Set by the storage context, which owns the catalog.
    /// </summary>
    internal Func<string, string, ClusteredKeyLayout?>? ClusteredKeys
    {
        get => _clusteredKeys;
        set
        {
            _clusteredKeys = value;
            _inner.ClusteredKeys = value;
        }
    }

    private Func<string, string, ClusteredKeyLayout?>? _clusteredKeys;

    public RowKey InsertRow(string databaseName, string tableName, byte[] rowBytes)
    {
        if (_clusteredKeys?.Invoke(databaseName, tableName) is not { } layout)
        {
            return _inner.InsertRow(databaseName, tableName, rowBytes);
        }

        // A clustered row is stored under its primary key; an existing live row with that key is a duplicate.
        RowKey key = layout.KeyOf(rowBytes, tableName);
        return _inner.TryInsertRowWithKey(databaseName, tableName, key, layout.StoredForm(rowBytes))
            ? key
            : throw DuplicateKey(key, tableName);
    }

    public List<RowKey> InsertRows(string databaseName, string tableName, List<byte[]> rowsBytes)
    {
        if (_clusteredKeys?.Invoke(databaseName, tableName) is not { } layout)
        {
            return _inner.InsertRows(databaseName, tableName, rowsBytes);
        }

        var keys = new List<RowKey>(rowsBytes.Count);
        var stored = new List<byte[]>(rowsBytes.Count);
        foreach (byte[] rowBytes in rowsBytes)
        {
            keys.Add(layout.KeyOf(rowBytes, tableName));
            stored.Add(layout.StoredForm(rowBytes));
        }

        return _inner.TryInsertRowsWithKeys(databaseName, tableName, keys, stored, out RowKey duplicate)
            ? keys
            : throw DuplicateKey(duplicate, tableName);
    }

    /// <summary>
    /// Inserts rows of a clustered table that the caller already keyed and serialized in their stored form
    /// (key cell NULL): the typed bulk path, which saves serializing a row twice.
    /// </summary>
    internal List<RowKey> InsertStoredRows(string databaseName, string tableName, List<RowKey> keys, List<byte[]> storedRows) =>
        _inner.TryInsertRowsWithKeys(databaseName, tableName, keys, storedRows, out RowKey duplicate)
            ? keys
            : throw DuplicateKey(duplicate, tableName);

    private static ConstraintViolationException DuplicateKey(RowKey key, string tableName) =>
        new($"Primary key violation: a row with key {key} already exists in table {tableName}.");
    // A clustered table stores rows without their key cell; reads put it back (see ClusteredKeyLayout).
    public byte[] ReadRow(string databaseName, string tableName, RowKey rowId) =>
        _inner.ReadRow(databaseName, tableName, rowId, _clusteredKeys?.Invoke(databaseName, tableName));

    public IEnumerable<(RowKey RowId, byte[] RawRow)> ReadAllRows(string databaseName, string tableName) =>
        _inner.ReadAllRows(databaseName, tableName, _clusteredKeys?.Invoke(databaseName, tableName));
    public void DeleteRow(string databaseName, string tableName, RowKey rowId) => _inner.DeleteRow(databaseName, tableName, rowId);
    public void DropTable(string databaseName, string tableName) => _inner.DropTable(databaseName, tableName);
    public void DropDatabase(string databaseName) => _inner.DropDatabase(databaseName);
    public List<(RowKey NewRowId, byte[] RawRow)> CompactTable(string databaseName, string tableName) =>
        _inner.CompactTable(databaseName, tableName, _clusteredKeys?.Invoke(databaseName, tableName));
    public bool HasAnyRows(string databaseName, string tableName) => _inner.HasAnyRows(databaseName, tableName);

    public long EstimateRowCount(string databaseName, string tableName) => _inner.EstimateRowCount(databaseName, tableName);

    bool IFixedWidthPatchStorageEngine.TryPatchFixedWidthRow(
        string databaseName,
        string tableName,
        RowKey rowId,
        IReadOnlyList<Column> columns,
        ReadOnlySpan<int> ordinals,
        ReadOnlySpan<DataVoFixedWidthValue> values) =>
        !PatchesClusteredKey(databaseName, tableName, ordinals)
        && ((IFixedWidthPatchStorageEngine)_inner).TryPatchFixedWidthRow(databaseName, tableName, rowId, columns, ordinals, values);

    int IFixedWidthPatchStorageEngine.TryPatchFixedWidthRows(
        string databaseName,
        string tableName,
        IReadOnlyList<Column> columns,
        ReadOnlySpan<int> ordinals,
        IReadOnlyList<FixedWidthPatchOperation> operations) =>
        PatchesClusteredKey(databaseName, tableName, ordinals)
            ? throw new InvalidOperationException("A batched in-place patch cannot change a clustered table's key; its callers only patch non-key columns.")
            : ((IFixedWidthPatchStorageEngine)_inner).TryPatchFixedWidthRows(databaseName, tableName, columns, ordinals, operations);

    /// <summary>
    /// An in-place patch keeps the row's key, so it must not change a clustered table's key column: such
    /// updates take the regular path, which moves the row to its new key.
    /// </summary>
    private bool PatchesClusteredKey(string databaseName, string tableName, ReadOnlySpan<int> ordinals) =>
        _clusteredKeys?.Invoke(databaseName, tableName) is { } layout && ordinals.IndexOf(layout.Ordinal) >= 0;

    /// <summary>Groups the current flow's mutations into one atomic commit-log frame (see LsmStorageEngine).</summary>
    internal IWriteBatchScope BeginWriteBatch() => _inner.BeginWriteBatch();

    public void Dispose()
    {
        if (_inner is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }
}
