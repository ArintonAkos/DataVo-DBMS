using DataVo.Core.StorageEngine.Backends.Abstractions;
using DataVo.Core.StorageEngine.Memory;

namespace DataVo.Core.StorageEngine.Backends;

internal sealed class InMemoryStorageBackend : IStorageBackend, IPagedRowScan, ITypedRowStorageEngine, IInMemoryStorageSnapshotProvider, IRowCountEstimator
{
    private readonly InMemoryStorageEngine _inner = new();

    public string BackendKind => "InMemory";

    public RowKey InsertRow(string databaseName, string tableName, byte[] rowBytes) => _inner.InsertRow(databaseName, tableName, rowBytes);
    public List<RowKey> InsertRows(string databaseName, string tableName, List<byte[]> rowsBytes) => RowKeyConversions.ToRowKeys(_inner.InsertRows(databaseName, tableName, rowsBytes));
    public long InsertTypedRow(string databaseName, string tableName, StoredRow row) =>
        ((ITypedRowStorageEngine)_inner).InsertTypedRow(databaseName, tableName, row);
    public List<long> InsertTypedRows(string databaseName, string tableName, IReadOnlyList<StoredRow> rows) =>
        ((ITypedRowStorageEngine)_inner).InsertTypedRows(databaseName, tableName, rows);
    public byte[] ReadRow(string databaseName, string tableName, RowKey rowId) => _inner.ReadRow(databaseName, tableName, (long)rowId);
    public bool TryReadTypedRow(string databaseName, string tableName, long rowId, out StoredRow? row) =>
        ((ITypedRowStorageEngine)_inner).TryReadTypedRow(databaseName, tableName, rowId, out row);
    public bool TryReadTypedCells(string databaseName, string tableName, long rowId, out ReadOnlySpan<DataVo.Core.Runtime.Reactive.CellValue> cells) =>
        ((ITypedRowStorageEngine)_inner).TryReadTypedCells(databaseName, tableName, rowId, out cells);
    public IEnumerable<(RowKey RowId, byte[] RawRow)> ReadAllRows(string databaseName, string tableName) => RowKeyConversions.ToRowKeys(_inner.ReadAllRows(databaseName, tableName));
    public IEnumerable<(long RowId, StoredRow Row)> ReadAllTypedRows(string databaseName, string tableName) =>
        ((ITypedRowStorageEngine)_inner).ReadAllTypedRows(databaseName, tableName);
    public bool HasAnyRows(string databaseName, string tableName) =>
        ((ITypedRowStorageEngine)_inner).HasAnyRows(databaseName, tableName);
    PageScan IPagedRowScan.VisitPage<TState>(string databaseName, string tableName, long? afterRowId, long? expectedGeneration, int maxRows, ref TState state, PagedRowVisitor<TState> visit) =>
        ((IPagedRowScan)_inner).VisitPage(databaseName, tableName, afterRowId, expectedGeneration, maxRows, ref state, visit);
    PageScan IPagedRowScan.ScanPage<TVisitor>(string databaseName, string tableName, long? afterRowId, long? expectedGeneration, int maxRows, ref TVisitor visitor) =>
        ((IPagedRowScan)_inner).ScanPage(databaseName, tableName, afterRowId, expectedGeneration, maxRows, ref visitor);
    public void DeleteRow(string databaseName, string tableName, RowKey rowId) => _inner.DeleteRow(databaseName, tableName, (long)rowId);
    public long EstimateRowCount(string databaseName, string tableName) => _inner.EstimateRowCount(databaseName, tableName);
    public void DropTable(string databaseName, string tableName) => _inner.DropTable(databaseName, tableName);
    public void DropDatabase(string databaseName) => _inner.DropDatabase(databaseName);
    public List<(RowKey NewRowId, byte[] RawRow)> CompactTable(string databaseName, string tableName) => RowKeyConversions.ToRowKeys(_inner.CompactTable(databaseName, tableName));
    public InMemoryStorageSnapshot CreateSnapshot() => ((IInMemoryStorageSnapshotProvider)_inner).CreateSnapshot();
    public void RestoreSnapshot(InMemoryStorageSnapshot snapshot) => ((IInMemoryStorageSnapshotProvider)_inner).RestoreSnapshot(snapshot);
}
