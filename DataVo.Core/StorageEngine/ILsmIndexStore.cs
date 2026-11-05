namespace DataVo.Core.StorageEngine;

/// <summary>
/// Persisted index storage offered by the LSM engine. Changes are staged in the caller's current write
/// unit (the statement or transaction batch), so they reach the commit log in the same frame as the row
/// changes and become visible to other sessions together with them. Keys are encoded with
/// <see cref="Lsm.V2.LsmIndexKeyCodec"/>.
/// </summary>
internal interface ILsmIndexStore
{
    void IndexPut(string databaseName, string tableName, string indexName, byte[] encodedKey, long rowId);

    void IndexDelete(string databaseName, string tableName, string indexName, byte[] encodedKey, long rowId);

    /// <summary>Row ids under <paramref name="encodedKey"/>, ascending; null when the index does not exist.</summary>
    List<long>? IndexLookup(string databaseName, string tableName, string indexName, byte[] encodedKey);

    /// <summary>Every live entry (null when the index does not exist); for the rare delete-by-row-id paths.</summary>
    List<(byte[] EncodedKey, long RowId)>? IndexEntries(string databaseName, string tableName, string indexName);

    void BuildIndex(string databaseName, string tableName, string indexName, IEnumerable<(byte[] EncodedKey, long RowId)> entries);

    void DropIndexTable(string databaseName, string tableName, string indexName);

    bool IsIndexBuilt(string databaseName, string tableName, string indexName);
}
