namespace DataVo.Core.Exceptions;

/// <summary>
/// Exception thrown when a row coordinate does not map to an existing row.
/// </summary>
public class RowNotFoundException : StorageException
{
    /// <summary>
    /// Gets the requested row identifier.
    /// </summary>
    public long RowId { get; }

    /// <summary>
    /// Gets the table name where the row lookup failed.
    /// </summary>
    public string TableName { get; }

    /// <summary>
    /// Initializes an exception for a missing row lookup.
    /// </summary>
    /// <param name="rowId">The requested row identifier.</param>
    /// <param name="tableName">The target table name.</param>
    public RowNotFoundException(long rowId, string tableName)
        : base($"Row {rowId} in table '{tableName}' was not found.")
    {
        RowId = rowId;
        TableName = tableName;
    }

    /// <summary>
    /// Initializes an exception for a missing row lookup with an inner exception.
    /// </summary>
    /// <param name="rowId">The requested row identifier.</param>
    /// <param name="tableName">The target table name.</param>
    /// <param name="innerException">The underlying cause.</param>
    public RowNotFoundException(long rowId, string tableName, Exception innerException)
        : base($"Row {rowId} in table '{tableName}' was not found.", innerException)
    {
        RowId = rowId;
        TableName = tableName;
    }
}