namespace DataVo.Core.StorageEngine;

internal interface ITypedRowStorageEngine
{
    long InsertTypedRow(string databaseName, string tableName, StoredRow row);

    List<long> InsertTypedRows(string databaseName, string tableName, IReadOnlyList<StoredRow> rows);

    bool TryReadTypedRow(string databaseName, string tableName, long rowId, out StoredRow? row);

    IEnumerable<(long RowId, StoredRow Row)> ReadAllTypedRows(string databaseName, string tableName);

    bool HasAnyRows(string databaseName, string tableName);
}
