using DataVo.Core.Models.Catalog;
using DataVo.Core.CompiledQueries;
using DataVo.Core.StorageEngine.Backends.Abstractions;
using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Core.StorageEngine.Backends;

internal sealed class LsmStorageBackend : IStorageBackend, IFixedWidthPatchStorageEngine, IRowExistenceProbe, IRowCountEstimator, IDisposable
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

    /// <summary>The underlying engine (test hooks and diagnostics).</summary>
    internal LsmStorageEngine Engine => _inner;

    /// <summary>The engine's persisted index storage.</summary>
    internal ILsmIndexStore IndexStore => _inner;

    public RowKey InsertRow(string databaseName, string tableName, byte[] rowBytes) => _inner.InsertRow(databaseName, tableName, rowBytes);
    public List<RowKey> InsertRows(string databaseName, string tableName, List<byte[]> rowsBytes) => RowKeyConversions.ToRowKeys(_inner.InsertRows(databaseName, tableName, rowsBytes));
    public byte[] ReadRow(string databaseName, string tableName, RowKey rowId) => _inner.ReadRow(databaseName, tableName, (long)rowId);
    public IEnumerable<(RowKey RowId, byte[] RawRow)> ReadAllRows(string databaseName, string tableName) => RowKeyConversions.ToRowKeys(_inner.ReadAllRows(databaseName, tableName));
    public void DeleteRow(string databaseName, string tableName, RowKey rowId) => _inner.DeleteRow(databaseName, tableName, (long)rowId);
    public void DropTable(string databaseName, string tableName) => _inner.DropTable(databaseName, tableName);
    public void DropDatabase(string databaseName) => _inner.DropDatabase(databaseName);
    public List<(RowKey NewRowId, byte[] RawRow)> CompactTable(string databaseName, string tableName) => RowKeyConversions.ToRowKeys(_inner.CompactTable(databaseName, tableName));
    public bool HasAnyRows(string databaseName, string tableName) => _inner.HasAnyRows(databaseName, tableName);

    public long EstimateRowCount(string databaseName, string tableName) => _inner.EstimateRowCount(databaseName, tableName);

    bool IFixedWidthPatchStorageEngine.TryPatchFixedWidthRow(
        string databaseName,
        string tableName,
        long rowId,
        IReadOnlyList<Column> columns,
        ReadOnlySpan<int> ordinals,
        ReadOnlySpan<DataVoFixedWidthValue> values) =>
        ((IFixedWidthPatchStorageEngine)_inner).TryPatchFixedWidthRow(databaseName, tableName, rowId, columns, ordinals, values);

    int IFixedWidthPatchStorageEngine.TryPatchFixedWidthRows(
        string databaseName,
        string tableName,
        IReadOnlyList<Column> columns,
        ReadOnlySpan<int> ordinals,
        IReadOnlyList<FixedWidthPatchOperation> operations) =>
        ((IFixedWidthPatchStorageEngine)_inner).TryPatchFixedWidthRows(databaseName, tableName, columns, ordinals, operations);

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
