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
    /// <param name="databaseName">The database that owns the target table.</param>
    /// <param name="tableName">The table receiving the row.</param>
    /// <param name="rowBytes">The serialized row payload.</param>
    /// <returns>The unique RowId identifying the location of this inserted byte array.</returns>
    long InsertRow(string databaseName, string tableName, byte[] rowBytes);

    /// <summary>
    /// Appends multiple serialized rows efficiently into the storage.
    /// </summary>
    /// <param name="databaseName">The database that owns the target table.</param>
    /// <param name="tableName">The table receiving the rows.</param>
    /// <param name="rowsBytes">The serialized row payloads.</param>
    /// <returns>A list of RowIds identifying the newly inserted records.</returns>
    List<long> InsertRows(string databaseName, string tableName, List<byte[]> rowsBytes);

    /// <summary>
    /// Fetches the raw byte array for a specific row by its physical RowId coordinate.
    /// </summary>
    /// <param name="databaseName">The database that owns the table.</param>
    /// <param name="tableName">The table containing the row.</param>
    /// <param name="rowId">The physical row identifier.</param>
    byte[] ReadRow(string databaseName, string tableName, long rowId);

    /// <summary>
    /// Retrieves a contiguous stream or list of all rows in the dataset (for Full Table Scans).
    /// </summary>
    /// <param name="databaseName">The database that owns the table.</param>
    /// <param name="tableName">The table to scan.</param>
    IEnumerable<(long RowId, byte[] RawRow)> ReadAllRows(string databaseName, string tableName);

    /// <summary>
    /// Deletes the row at the given physical coordinate.
    /// </summary>
    /// <param name="databaseName">The database that owns the table.</param>
    /// <param name="tableName">The table containing the row.</param>
    /// <param name="rowId">The physical row identifier.</param>
    void DeleteRow(string databaseName, string tableName, long rowId);

    /// <summary>
    /// Clears an entire table from storage.
    /// </summary>
    /// <param name="databaseName">The database that owns the table.</param>
    /// <param name="tableName">The table to drop.</param>
    void DropTable(string databaseName, string tableName);

    /// <summary>
    /// Drops all data belonging to a database from storage.
    /// </summary>
    /// <param name="databaseName">The database to drop.</param>
    void DropDatabase(string databaseName);

    /// <summary>
    /// Compacts the table by removing tombstoned/deleted rows and rewriting data contiguously.
    /// Returns the list of (newRowId, rawRow) for all surviving rows.
    /// </summary>
    /// <param name="databaseName">The database that owns the table.</param>
    /// <param name="tableName">The table to compact.</param>
    List<(long NewRowId, byte[] RawRow)> CompactTable(string databaseName, string tableName);
}
