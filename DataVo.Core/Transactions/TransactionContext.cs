namespace DataVo.Core.Transactions;

using DataVo.Core.MVCC;

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
    private readonly Dictionary<string, BufferedStateSnapshot> _savepoints = new(StringComparer.OrdinalIgnoreCase);
    private readonly List<string> _savepointOrder = [];

    private sealed class BufferedStateSnapshot
    {
        public required Dictionary<string, List<Dictionary<string, dynamic>>> InsertedRows { get; init; }
        public required Dictionary<string, HashSet<long>> DeletedRowIds { get; init; }
        public required Dictionary<string, List<(long RowId, Dictionary<string, dynamic> UpdatedColumns)>> UpdatedRows { get; init; }
    }

    /// <summary>
    /// Unique transaction ID assigned at BEGIN time. Used for MVCC versioning.
    /// </summary>
    public long TransactionId { get; set; }

    /// <summary>
    /// The snapshot created at BEGIN time. Defines which row versions are visible to this transaction.
    /// </summary>
    public TransactionSnapshot? Snapshot { get; set; }

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

    /// <summary>
    /// Captures the current buffered DML state under the supplied savepoint name.
    /// Reusing a name updates the savepoint to the latest state.
    /// </summary>
    public void CreateSavepoint(string savepointName)
    {
        if (string.IsNullOrWhiteSpace(savepointName))
            throw new ArgumentException("Savepoint name is required.", nameof(savepointName));

        _savepoints[savepointName] = CaptureSnapshot();
        _savepointOrder.RemoveAll(name => string.Equals(name, savepointName, StringComparison.OrdinalIgnoreCase));
        _savepointOrder.Add(savepointName);
    }

    /// <summary>
    /// Restores the buffered DML state from a previously created savepoint.
    /// Savepoints created after the restored savepoint are discarded.
    /// </summary>
    public void RollbackToSavepoint(string savepointName)
    {
        if (string.IsNullOrWhiteSpace(savepointName))
            throw new ArgumentException("Savepoint name is required.", nameof(savepointName));

        if (!_savepoints.TryGetValue(savepointName, out BufferedStateSnapshot? snapshot))
            throw new InvalidOperationException($"Savepoint '{savepointName}' does not exist.");

        RestoreSnapshot(snapshot);

        int index = _savepointOrder.FindLastIndex(name => string.Equals(name, savepointName, StringComparison.OrdinalIgnoreCase));
        for (int i = _savepointOrder.Count - 1; i > index; i--)
        {
            string trailingSavepoint = _savepointOrder[i];
            _savepoints.Remove(trailingSavepoint);
            _savepointOrder.RemoveAt(i);
        }
    }

    /// <summary>
    /// Removes a previously created savepoint while leaving transaction buffers unchanged.
    /// </summary>
    public void ReleaseSavepoint(string savepointName)
    {
        if (string.IsNullOrWhiteSpace(savepointName))
            throw new ArgumentException("Savepoint name is required.", nameof(savepointName));

        if (!_savepoints.Remove(savepointName))
            throw new InvalidOperationException($"Savepoint '{savepointName}' does not exist.");

        _savepointOrder.RemoveAll(name => string.Equals(name, savepointName, StringComparison.OrdinalIgnoreCase));
    }

    private BufferedStateSnapshot CaptureSnapshot()
    {
        return new BufferedStateSnapshot
        {
            InsertedRows = CloneInsertedRows(InsertedRows),
            DeletedRowIds = CloneDeletedRows(DeletedRowIds),
            UpdatedRows = CloneUpdatedRows(UpdatedRows)
        };
    }

    private void RestoreSnapshot(BufferedStateSnapshot snapshot)
    {
        InsertedRows.Clear();
        foreach ((string tableName, List<Dictionary<string, dynamic>> rows) in CloneInsertedRows(snapshot.InsertedRows))
        {
            InsertedRows[tableName] = rows;
        }

        DeletedRowIds.Clear();
        foreach ((string tableName, HashSet<long> rowIds) in CloneDeletedRows(snapshot.DeletedRowIds))
        {
            DeletedRowIds[tableName] = rowIds;
        }

        UpdatedRows.Clear();
        foreach ((string tableName, List<(long RowId, Dictionary<string, dynamic> UpdatedColumns)> rows) in CloneUpdatedRows(snapshot.UpdatedRows))
        {
            UpdatedRows[tableName] = rows;
        }
    }

    private static Dictionary<string, List<Dictionary<string, dynamic>>> CloneInsertedRows(
        Dictionary<string, List<Dictionary<string, dynamic>>> source)
    {
        var clone = new Dictionary<string, List<Dictionary<string, dynamic>>>(source.Comparer);
        foreach ((string tableName, List<Dictionary<string, dynamic>> rows) in source)
        {
            clone[tableName] = rows.Select(CloneRow).ToList();
        }

        return clone;
    }

    private static Dictionary<string, HashSet<long>> CloneDeletedRows(
        Dictionary<string, HashSet<long>> source)
    {
        var clone = new Dictionary<string, HashSet<long>>(source.Comparer);
        foreach ((string tableName, HashSet<long> rowIds) in source)
        {
            clone[tableName] = new HashSet<long>(rowIds, rowIds.Comparer);
        }

        return clone;
    }

    private static Dictionary<string, List<(long RowId, Dictionary<string, dynamic> UpdatedColumns)>> CloneUpdatedRows(
        Dictionary<string, List<(long RowId, Dictionary<string, dynamic> UpdatedColumns)>> source)
    {
        var clone = new Dictionary<string, List<(long RowId, Dictionary<string, dynamic> UpdatedColumns)>>(source.Comparer);
        foreach ((string tableName, List<(long RowId, Dictionary<string, dynamic> UpdatedColumns)> updates) in source)
        {
            clone[tableName] = updates
                .Select(update => (update.RowId, CloneRow(update.UpdatedColumns)))
                .ToList();
        }

        return clone;
    }

    private static Dictionary<string, dynamic> CloneRow(Dictionary<string, dynamic> row)
    {
        return new Dictionary<string, dynamic>(row, row.Comparer);
    }
}
