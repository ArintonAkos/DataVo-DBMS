namespace DataVo.Core.Transactions;

using DataVo.Core.StorageEngine;
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
        public required Dictionary<string, List<Dictionary<string, object?>>> InsertedRows { get; init; }
        public required Dictionary<string, HashSet<RowKey>> DeletedRowIds { get; init; }
        public required Dictionary<string, List<(RowKey RowId, Dictionary<string, object?> UpdatedColumns)>> UpdatedRows { get; init; }
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
    public Dictionary<string, List<Dictionary<string, object?>>> InsertedRows { get; } = new();

    /// <summary>
    /// Buffered DELETE operations grouped by table name.
    /// Each entry maps a table name to a set of row IDs marked for deletion.
    /// </summary>
    public Dictionary<string, HashSet<RowKey>> DeletedRowIds { get; } = new();

    /// <summary>
    /// Buffered UPDATE operations grouped by table name.
    /// Each entry maps a table name to a list of (RowId, UpdatedColumns) tuples.
    /// </summary>
    public Dictionary<string, List<(RowKey RowId, Dictionary<string, object?> UpdatedColumns)>> UpdatedRows { get; } = new();

    /// <summary>
    /// The high half of the reserved row keys reads give a buffered insert, which has no storage key yet: the
    /// insert at position <c>n</c> of a table's buffer is read as <c>RowKey(BufferedRowKeyHigh, n)</c>.
    /// </summary>
    internal const ulong BufferedRowKeyHigh = ulong.MaxValue;

    /// <summary>Gets whether a row key names one of a transaction's buffered inserts rather than a stored row.</summary>
    internal static bool IsBufferedRowKey(RowKey rowId) => rowId.High == BufferedRowKeyHigh;

    /// <summary>Gets the reserved row key reads give the buffered insert at <paramref name="position"/>.</summary>
    internal static RowKey BufferedRowKey(int position) => new(BufferedRowKeyHigh, (ulong)position);

    /// <summary>Gets whether this transaction has uncommitted writes to a table.</summary>
    /// <param name="tableName">The table name.</param>
    /// <returns><see langword="true"/> when a buffered insert, update or delete exists.</returns>
    public bool HasBufferedWrites(string tableName) =>
        (InsertedRows.TryGetValue(tableName, out var inserted) && inserted.Count > 0)
        || (DeletedRowIds.TryGetValue(tableName, out var deleted) && deleted.Count > 0)
        || (UpdatedRows.TryGetValue(tableName, out var updated) && updated.Count > 0);

    /// <summary>
    /// Appends a row to the insert buffer for the specified table.
    /// </summary>
    /// <param name="tableName">The target table name.</param>
    /// <param name="row">The row data as a column-value dictionary.</param>
    public void BufferInsert(string tableName, Dictionary<string, object?> row)
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
    public void BufferDelete(string tableName, RowKey rowId)
    {
        if (!DeletedRowIds.ContainsKey(tableName))
            DeletedRowIds[tableName] = [];

        DeletedRowIds[tableName].Add(rowId);

        // An update buffered earlier for this row is void: COMMIT deletes the row, and a row inserted later
        // with the same key must not receive it.
        if (UpdatedRows.TryGetValue(tableName, out var updates))
        {
            updates.RemoveAll(entry => entry.RowId == rowId);
        }
    }

    /// <summary>
    /// Removes the rows this transaction has already deleted from a statement's candidate rows. The statement
    /// reads committed storage, which still holds them until COMMIT, so without this an UPDATE or DELETE would
    /// count them as changed again.
    /// </summary>
    /// <param name="tableName">The table the statement changes.</param>
    /// <param name="rowIds">The rows the statement's condition matched; rows deleted earlier are removed in place.</param>
    public void ExcludeDeleted(string tableName, List<RowKey> rowIds)
    {
        if (DeletedRowIds.TryGetValue(tableName, out HashSet<RowKey>? deleted) && deleted.Count > 0)
        {
            rowIds.RemoveAll(deleted.Contains);
        }
    }

    /// <summary>
    /// Buffers an update of a stored row. Updates of the same row coalesce into one entry holding the row's latest
    /// values, so later reads and COMMIT see a single, current version of it.
    /// </summary>
    /// <param name="tableName">The target table name.</param>
    /// <param name="rowId">The physical row ID being updated.</param>
    /// <param name="updatedColumns">The column-value pairs to overwrite (usually the whole new row).</param>
    public void BufferUpdate(string tableName, RowKey rowId, Dictionary<string, object?> updatedColumns)
    {
        if (!UpdatedRows.TryGetValue(tableName, out var updates))
        {
            updates = [];
            UpdatedRows[tableName] = updates;
        }

        for (int i = 0; i < updates.Count; i++)
        {
            if (updates[i].RowId == rowId)
            {
                Dictionary<string, object?> merged = updates[i].UpdatedColumns;
                foreach (KeyValuePair<string, object?> column in updatedColumns)
                {
                    merged[column.Key] = column.Value;
                }

                return;
            }
        }

        updates.Add((rowId, updatedColumns));
    }

    /// <summary>Replaces a buffered insert with its updated values (an UPDATE of a row this transaction inserted).</summary>
    /// <param name="tableName">The target table name.</param>
    /// <param name="rowId">The buffered row's reserved key (see <see cref="BufferedRowKey"/>).</param>
    /// <param name="row">The row's new values.</param>
    internal void ReplaceBufferedInsert(string tableName, RowKey rowId, Dictionary<string, object?> row)
    {
        List<Dictionary<string, object?>> rows = InsertedRows[tableName];
        Dictionary<string, object?> current = rows[(int)rowId.Low];
        foreach (KeyValuePair<string, object?> column in row)
        {
            current[column.Key] = column.Value;
        }
    }

    /// <summary>
    /// Drops buffered inserts (a DELETE of rows this transaction inserted). Positions above a removed row shift
    /// down, so reserved keys are only meaningful within one statement.
    /// </summary>
    /// <param name="tableName">The target table name.</param>
    /// <param name="rowIds">The buffered rows' reserved keys.</param>
    internal void RemoveBufferedInserts(string tableName, IEnumerable<RowKey> rowIds)
    {
        if (!InsertedRows.TryGetValue(tableName, out List<Dictionary<string, object?>>? rows))
        {
            return;
        }

        foreach (int position in rowIds.Select(rowId => (int)rowId.Low).Distinct().OrderByDescending(position => position))
        {
            if (position < rows.Count)
            {
                rows.RemoveAt(position);
            }
        }
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

        int index = -1;
        for (int i = _savepointOrder.Count - 1; i >= 0; i--)
        {
            if (string.Equals(_savepointOrder[i], savepointName, StringComparison.OrdinalIgnoreCase))
            {
                index = i;
                break;
            }
        }

        if (index < 0)
        {
            throw new InvalidOperationException($"Savepoint '{savepointName}' order marker was not found.");
        }

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
        foreach ((string tableName, List<Dictionary<string, object?>> rows) in CloneInsertedRows(snapshot.InsertedRows))
        {
            InsertedRows[tableName] = rows;
        }

        DeletedRowIds.Clear();
        foreach ((string tableName, HashSet<RowKey> rowIds) in CloneDeletedRows(snapshot.DeletedRowIds))
        {
            DeletedRowIds[tableName] = rowIds;
        }

        UpdatedRows.Clear();
        foreach ((string tableName, List<(RowKey RowId, Dictionary<string, object?> UpdatedColumns)> rows) in CloneUpdatedRows(snapshot.UpdatedRows))
        {
            UpdatedRows[tableName] = rows;
        }
    }

    private static Dictionary<string, List<Dictionary<string, object?>>> CloneInsertedRows(
        Dictionary<string, List<Dictionary<string, object?>>> source)
    {
        var clone = new Dictionary<string, List<Dictionary<string, object?>>>(source.Comparer);
        foreach ((string tableName, List<Dictionary<string, object?>> rows) in source)
        {
            clone[tableName] = rows.Select(CloneRow).ToList();
        }

        return clone;
    }

    private static Dictionary<string, HashSet<RowKey>> CloneDeletedRows(
        Dictionary<string, HashSet<RowKey>> source)
    {
        var clone = new Dictionary<string, HashSet<RowKey>>(source.Comparer);
        foreach ((string tableName, HashSet<RowKey> rowIds) in source)
        {
            clone[tableName] = new HashSet<RowKey>(rowIds, rowIds.Comparer);
        }

        return clone;
    }

    private static Dictionary<string, List<(RowKey RowId, Dictionary<string, object?> UpdatedColumns)>> CloneUpdatedRows(
        Dictionary<string, List<(RowKey RowId, Dictionary<string, object?> UpdatedColumns)>> source)
    {
        var clone = new Dictionary<string, List<(RowKey RowId, Dictionary<string, object?> UpdatedColumns)>>(source.Comparer);
        foreach ((string tableName, List<(RowKey RowId, Dictionary<string, object?> UpdatedColumns)> updates) in source)
        {
            clone[tableName] = updates
                .Select(update => (update.RowId, CloneRow(update.UpdatedColumns)))
                .ToList();
        }

        return clone;
    }

    private static Dictionary<string, object?> CloneRow(Dictionary<string, object?> row)
    {
        return new Dictionary<string, object?>(row, row.Comparer);
    }
}
