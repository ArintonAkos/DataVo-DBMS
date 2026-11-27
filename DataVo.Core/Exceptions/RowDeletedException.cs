namespace DataVo.Core.Exceptions;

/// <summary>
/// Thrown when an attempt is made to read a row that has been logically deleted (tombstoned).
/// A dedicated type enables <see cref="StorageEngine.StorageContext.TableContainsRow"/> to distinguish a
/// deleted row from an unexpected I/O failure.
/// </summary>
public class RowDeletedException : StorageException
{
    /// <summary>
    /// Initializes a new instance for the specified row identifier.
    /// </summary>
    /// <param name="rowId">The key of the row that was deleted.</param>
    /// <param name="tableName">The table containing the deleted row.</param>
    public RowDeletedException(RowKey rowId, string tableName)
        : base($"Row {rowId} in table '{tableName}' has been deleted.")
    {
        RowId = rowId;
        TableName = tableName;
    }

    /// <summary>
    /// Gets the key of the deleted row.
    /// </summary>
    public RowKey RowId { get; }

    /// <summary>
    /// Gets the table that contained the deleted row.
    /// </summary>
    public string TableName { get; }
}
