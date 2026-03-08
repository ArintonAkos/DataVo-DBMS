namespace DataVo.Core.Transactions;

/// <summary>
/// A volatile in-memory buffer that accumulates DML operations (inserts, updates, deletes)
/// within an explicit transaction boundary. Changes stored here are invisible to other sessions
/// until a <c>COMMIT</c> flushes them to the physical storage engine.
/// <para>
/// On <c>ROLLBACK</c>, this context is simply discarded from memory with zero disk I/O cost.
/// </para>
/// </summary>
public class TransactionContext
{
    /// <summary>
    /// Buffered INSERT operations grouped by table name.
    /// Each entry maps a table name to a list of row dictionaries awaiting physical insertion.
    /// </summary>
    public Dictionary<string, List<Dictionary<string, dynamic>>> InsertedRows { get; } = new();

    /// <summary>
    /// Buffered DELETE operations grouped by table name.
    /// Each entry maps a table name to a set of row IDs marked for deletion.
    /// </summary>
    public Dictionary<string, HashSet<long>> DeletedRowIds { get; } = new();

    /// <summary>
    /// Buffered UPDATE operations grouped by table name.
    /// Each entry maps a table name to a list of (RowId, UpdatedColumns) tuples.
    /// </summary>
    public Dictionary<string, List<(long RowId, Dictionary<string, dynamic> UpdatedColumns)>> UpdatedRows { get; } = new();

    /// <summary>
    /// Appends a row to the insert buffer for the specified table.
    /// </summary>
    /// <param name="tableName">The target table name.</param>
    /// <param name="row">The row data as a column-value dictionary.</param>
    public void BufferInsert(string tableName, Dictionary<string, dynamic> row)
    {
        if (!InsertedRows.ContainsKey(tableName))
            InsertedRows[tableName] = [];

        InsertedRows[tableName].Add(row);
    }

    /// <summary>
    /// Marks a row ID for deletion in the specified table.
    /// </summary>
    /// <param name="tableName">The target table name.</param>
    /// <param name="rowId">The physical row ID to delete.</param>
    public void BufferDelete(string tableName, long rowId)
    {
        if (!DeletedRowIds.ContainsKey(tableName))
            DeletedRowIds[tableName] = [];

        DeletedRowIds[tableName].Add(rowId);
    }

    /// <summary>
    /// Buffers a column update for the specified row in the specified table.
    /// </summary>
    /// <param name="tableName">The target table name.</param>
    /// <param name="rowId">The physical row ID being updated.</param>
    /// <param name="updatedColumns">The column-value pairs to overwrite.</param>
    public void BufferUpdate(string tableName, long rowId, Dictionary<string, dynamic> updatedColumns)
    {
        if (!UpdatedRows.ContainsKey(tableName))
            UpdatedRows[tableName] = [];

        UpdatedRows[tableName].Add((rowId, updatedColumns));
    }
}
