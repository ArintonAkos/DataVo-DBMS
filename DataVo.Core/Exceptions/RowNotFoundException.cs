namespace DataVo.Core.Exceptions;

/// <summary>
/// Exception thrown when a row coordinate does not map to an existing row.
/// </summary>
public class RowNotFoundException : StorageException
{
    public long RowId { get; }
    public string TableName { get; }

    public RowNotFoundException(long rowId, string tableName)
        : base($"Row {rowId} in table '{tableName}' was not found.")
    {
        RowId = rowId;
        TableName = tableName;
    }

    public RowNotFoundException(long rowId, string tableName, Exception innerException)
        : base($"Row {rowId} in table '{tableName}' was not found.", innerException)
    {
        RowId = rowId;
        TableName = tableName;
    }
}