namespace DataVo.Core.StorageEngine;

/// <summary>
/// A generic interface that strictly handles appending, fetching, and removing 
/// bytes (rows) from the physical storage medium underneath the DBMS (e.g. Memory vs Disk).
/// </summary>
public interface IStorageEngine
{
    /// <summary>
    /// Appends the serialized byte representation of a single row into the storage.
    /// </summary>
    /// <returns>The unique RowId identifying the location of this inserted byte array</returns>
    long InsertRow(string databaseName, string tableName, byte[] rowBytes);

    /// <summary>
    /// Appends multiple serialized rows efficiently into the storage.
    /// </summary>
    /// <returns>A list of RowIds identifying the newly inserted records</returns>
    List<long> InsertRows(string databaseName, string tableName, List<byte[]> rowsBytes);

    /// <summary>
    /// Fetches the raw byte array for a specific row by its physical RowId coordinate.
    /// </summary>
    byte[] ReadRow(string databaseName, string tableName, long rowId);

    /// <summary>
    /// Retrieves a contiguous stream or list of all rows in the dataset (for Full Table Scans).
    /// </summary>
    IEnumerable<(long RowId, byte[] RawRow)> ReadAllRows(string databaseName, string tableName);

    /// <summary>
    /// Deletes the row at the given physical coordinate.
    /// </summary>
    void DeleteRow(string databaseName, string tableName, long rowId);

    /// <summary>
    /// Clears an entire table from storage.
    /// </summary>
    void DropTable(string databaseName, string tableName);
}
