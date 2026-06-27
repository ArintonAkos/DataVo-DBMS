using DataVo.Core.StorageEngine.Backends.Abstractions;
using DataVo.Core.StorageEngine.Memory;

namespace DataVo.Core.StorageEngine.Backends;

internal sealed class InMemoryStorageBackend : IStorageBackend, ITypedRowStorageEngine, IInMemoryStorageSnapshotProvider
{
    private readonly InMemoryStorageEngine _inner = new();

    public string BackendKind => "InMemory";

    public long InsertRow(string databaseName, string tableName, byte[] rowBytes) => _inner.InsertRow(databaseName, tableName, rowBytes);
    public List<long> InsertRows(string databaseName, string tableName, List<byte[]> rowsBytes) => _inner.InsertRows(databaseName, tableName, rowsBytes);
    public long InsertTypedRow(string databaseName, string tableName, StoredRow row) =>
        ((ITypedRowStorageEngine)_inner).InsertTypedRow(databaseName, tableName, row);
    public List<long> InsertTypedRows(string databaseName, string tableName, IReadOnlyList<StoredRow> rows) =>
        ((ITypedRowStorageEngine)_inner).InsertTypedRows(databaseName, tableName, rows);
    public byte[] ReadRow(string databaseName, string tableName, long rowId) => _inner.ReadRow(databaseName, tableName, rowId);
    public bool TryReadTypedRow(string databaseName, string tableName, long rowId, out StoredRow? row) =>
        ((ITypedRowStorageEngine)_inner).TryReadTypedRow(databaseName, tableName, rowId, out row);
    public IEnumerable<(long RowId, byte[] RawRow)> ReadAllRows(string databaseName, string tableName) => _inner.ReadAllRows(databaseName, tableName);
    public IEnumerable<(long RowId, StoredRow Row)> ReadAllTypedRows(string databaseName, string tableName) =>
        ((ITypedRowStorageEngine)_inner).ReadAllTypedRows(databaseName, tableName);
    public bool HasAnyRows(string databaseName, string tableName) =>
        ((ITypedRowStorageEngine)_inner).HasAnyRows(databaseName, tableName);
    public void DeleteRow(string databaseName, string tableName, long rowId) => _inner.DeleteRow(databaseName, tableName, rowId);
    public void DropTable(string databaseName, string tableName) => _inner.DropTable(databaseName, tableName);
    public void DropDatabase(string databaseName) => _inner.DropDatabase(databaseName);
    public List<(long NewRowId, byte[] RawRow)> CompactTable(string databaseName, string tableName) => _inner.CompactTable(databaseName, tableName);
    public InMemoryStorageSnapshot CreateSnapshot() => ((IInMemoryStorageSnapshotProvider)_inner).CreateSnapshot();
    public void RestoreSnapshot(InMemoryStorageSnapshot snapshot) => ((IInMemoryStorageSnapshotProvider)_inner).RestoreSnapshot(snapshot);
}
