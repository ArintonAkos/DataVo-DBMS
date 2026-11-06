using DataVo.Core.StorageEngine.Backends.Abstractions;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.StorageEngine.Disk;

namespace DataVo.Core.StorageEngine.Backends;

internal sealed class DiskStorageBackend : IStorageBackend, IDisposable
{
    private readonly DiskStorageEngine _inner;

    public DiskStorageBackend(string storagePath, bool syncWrites = false, IoSchedulerMode ioSchedulerMode = IoSchedulerMode.Off)
    {
        IoSchedulerMode = ioSchedulerMode;
        _inner = new DiskStorageEngine(storagePath, syncWrites, ioSchedulerMode);
    }

    public string BackendKind => "Disk";

    internal IoSchedulerMode IoSchedulerMode { get; }

    public void CreateTable(string databaseName, string tableName) => _inner.CreateTable(databaseName, tableName);

    public RowKey InsertRow(string databaseName, string tableName, byte[] rowBytes) => _inner.InsertRow(databaseName, tableName, rowBytes);
    public List<RowKey> InsertRows(string databaseName, string tableName, List<byte[]> rowsBytes) => RowKeyConversions.ToRowKeys(_inner.InsertRows(databaseName, tableName, rowsBytes));
    public byte[] ReadRow(string databaseName, string tableName, RowKey rowId) => _inner.ReadRow(databaseName, tableName, (long)rowId);
    public IEnumerable<(RowKey RowId, byte[] RawRow)> ReadAllRows(string databaseName, string tableName) => RowKeyConversions.ToRowKeys(_inner.ReadAllRows(databaseName, tableName));
    public void DeleteRow(string databaseName, string tableName, RowKey rowId) => _inner.DeleteRow(databaseName, tableName, (long)rowId);
    public void DropTable(string databaseName, string tableName) => _inner.DropTable(databaseName, tableName);
    public void DropDatabase(string databaseName) => _inner.DropDatabase(databaseName);
    public List<(RowKey NewRowId, byte[] RawRow)> CompactTable(string databaseName, string tableName) => RowKeyConversions.ToRowKeys(_inner.CompactTable(databaseName, tableName));
    public void FlushToDisk() => _inner.FlushToDisk();
    public void Dispose() => _inner.Dispose();
}
