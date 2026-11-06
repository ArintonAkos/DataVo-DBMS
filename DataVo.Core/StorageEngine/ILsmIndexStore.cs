namespace DataVo.Core.StorageEngine;

/// <summary>
/// Persisted index storage offered by the LSM engine. Changes are staged in the caller's current write
/// unit (the statement or transaction batch), so they reach the commit log in the same frame as the row
/// changes and become visible to other sessions together with them. Keys are encoded with
/// <see cref="Lsm.V2.LsmIndexKeyCodec"/>.
/// </summary>
internal interface ILsmIndexStore
{
    void IndexPut(string databaseName, string tableName, string indexName, byte[] encodedKey, RowKey rowId);

    void IndexDelete(string databaseName, string tableName, string indexName, byte[] encodedKey, RowKey rowId);

    /// <summary>Row ids under <paramref name="encodedKey"/>, ascending; null when the index does not exist.</summary>
    List<RowKey>? IndexLookup(string databaseName, string tableName, string indexName, byte[] encodedKey);

    /// <summary>Every live entry (null when the index does not exist); for the rare delete-by-row-id paths.</summary>
    List<(byte[] EncodedKey, RowKey RowId)>? IndexEntries(string databaseName, string tableName, string indexName);

    void BuildIndex(string databaseName, string tableName, string indexName, IEnumerable<(byte[] EncodedKey, RowKey RowId)> entries);

    void DropIndexTable(string databaseName, string tableName, string indexName);

    bool IsIndexBuilt(string databaseName, string tableName, string indexName);

    /// <summary>
    /// Whether the index is a clustered table's primary key, which the rows themselves serve: its writes
    /// are dropped, so callers can skip encoding them.
    /// </summary>
    bool IsRowKeyIndex(string databaseName, string tableName, string indexName);

    /// <summary>For a <see cref="IsRowKeyIndex"/> index: whether a live row has <paramref name="key"/>, without allocating.</summary>
    bool RowKeyIndexContains(string databaseName, string tableName, RowKey key);
}
