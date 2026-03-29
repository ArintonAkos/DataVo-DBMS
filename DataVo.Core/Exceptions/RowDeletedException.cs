namespace DataVo.Core.Exceptions;

/// <summary>
/// Thrown when an attempt is made to read a row that has been logically deleted (tombstoned).
/// This exception replaces the generic <see cref="Exception"/> previously thrown by the disk storage
/// engine, enabling <see cref="StorageEngine.StorageContext.TableContainsRow"/> to distinguish a
/// deleted row from an unexpected I/O failure.
/// </summary>
public class RowDeletedException : StorageException
{
    /// <summary>
    /// Initializes a new instance for the specified row identifier.
    /// </summary>
    /// <param name="rowId">The byte-offset row identifier that was tombstoned.</param>
    /// <param name="tableName">The table containing the deleted row.</param>
    public RowDeletedException(long rowId, string tableName)
        : base($"Row {rowId} in table '{tableName}' has been deleted.")
    {
        RowId = rowId;
        TableName = tableName;
    }

    /// <summary>
    /// Gets the byte-offset identifier of the deleted row.
    /// </summary>
    public long RowId { get; }

    /// <summary>
    /// Gets the table that contained the deleted row.
    /// </summary>
    public string TableName { get; }
}
