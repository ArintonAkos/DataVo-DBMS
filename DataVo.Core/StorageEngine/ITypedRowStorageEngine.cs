namespace DataVo.Core.StorageEngine;

internal interface ITypedRowStorageEngine
{
    long InsertTypedRow(string databaseName, string tableName, StoredRow row);

    List<long> InsertTypedRows(string databaseName, string tableName, IReadOnlyList<StoredRow> rows);

    bool TryReadTypedRow(string databaseName, string tableName, long rowId, out StoredRow? row);

    /// <summary>The cells of a live typed row, borrowed from the store: read them at once and do not keep them.</summary>
    bool TryReadTypedCells(string databaseName, string tableName, long rowId, out ReadOnlySpan<DataVo.Core.Runtime.Reactive.CellValue> cells);

    IEnumerable<(long RowId, StoredRow Row)> ReadAllTypedRows(string databaseName, string tableName);

    bool HasAnyRows(string databaseName, string tableName);
}
