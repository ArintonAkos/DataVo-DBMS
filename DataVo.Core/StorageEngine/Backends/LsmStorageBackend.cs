using DataVo.Core.Models.Catalog;
using DataVo.Core.CompiledQueries;
using DataVo.Core.StorageEngine.Backends.Abstractions;
using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Core.StorageEngine.Backends;

internal sealed class LsmStorageBackend : IStorageBackend, IFixedWidthPatchStorageEngine, IRowExistenceProbe, IDisposable
{
    private readonly LsmStorageEngine _inner;

    public LsmStorageBackend(
        string storagePath,
        LsmWalDurabilityMode walDurabilityMode = LsmWalDurabilityMode.StrictFsync)
    {
        _inner = new LsmStorageEngine(storagePath, walDurabilityMode);
    }

    public string BackendKind => "Lsm";

    public long InsertRow(string databaseName, string tableName, byte[] rowBytes) => _inner.InsertRow(databaseName, tableName, rowBytes);
    public List<long> InsertRows(string databaseName, string tableName, List<byte[]> rowsBytes) => _inner.InsertRows(databaseName, tableName, rowsBytes);
    public byte[] ReadRow(string databaseName, string tableName, long rowId) => _inner.ReadRow(databaseName, tableName, rowId);
    public IEnumerable<(long RowId, byte[] RawRow)> ReadAllRows(string databaseName, string tableName) => _inner.ReadAllRows(databaseName, tableName);
    public void DeleteRow(string databaseName, string tableName, long rowId) => _inner.DeleteRow(databaseName, tableName, rowId);
    public void DropTable(string databaseName, string tableName) => _inner.DropTable(databaseName, tableName);
    public void DropDatabase(string databaseName) => _inner.DropDatabase(databaseName);
    public List<(long NewRowId, byte[] RawRow)> CompactTable(string databaseName, string tableName) => _inner.CompactTable(databaseName, tableName);
    public bool HasAnyRows(string databaseName, string tableName) => _inner.HasAnyRows(databaseName, tableName);

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

    public void Dispose()
    {
        if (_inner is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }
}
