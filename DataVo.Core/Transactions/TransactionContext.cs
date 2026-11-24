namespace DataVo.Core.Transactions;

using System.Diagnostics.CodeAnalysis;
using DataVo.Core.BTree;
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
    private static readonly RowKey[] EmptyRowKeys = [];

    private readonly Dictionary<string, BufferedStateSnapshot> _savepoints = new(StringComparer.OrdinalIgnoreCase);
    private readonly List<string> _savepointOrder = [];
    private readonly Dictionary<string, Dictionary<RowKey, int>> _updatedIndexByRowId = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<(string TableName, string ColumnName), BufferedColumnIndex> _columnIndexes = new(TableColumnComparer.Instance);
    private readonly Dictionary<string, TablePendingKeyState> _pendingKeysByTable = new(StringComparer.OrdinalIgnoreCase);

    private sealed class BufferedStateSnapshot
    {
        public required Dictionary<string, List<Dictionary<string, object?>>> InsertedRows { get; init; }
        public required Dictionary<string, HashSet<RowKey>> DeletedRowIds { get; init; }
        public required Dictionary<string, List<(RowKey RowId, Dictionary<string, object?> UpdatedColumns)>> UpdatedRows { get; init; }
    }

    private sealed class BufferedColumnIndex
    {
        private readonly Dictionary<string, HashSet<RowKey>> _rowsByKey = new(StringComparer.Ordinal);
        private readonly Dictionary<RowKey, string> _keyByRowId = [];

        public void Upsert(RowKey rowId, IReadOnlyDictionary<string, object?> row, string columnName)
        {
            Remove(rowId);
            if (!IndexKeyEncoder.TryBuildSingleColumnKey(row, columnName, out string key))
            {
                return;
            }

            if (!_rowsByKey.TryGetValue(key, out HashSet<RowKey>? set))
            {
                set = [];
                _rowsByKey[key] = set;
            }

            set.Add(rowId);
            _keyByRowId[rowId] = key;
        }

        public void Remove(RowKey rowId)
        {
            if (!_keyByRowId.Remove(rowId, out string? oldKey))
            {
                return;
            }

            if (_rowsByKey.TryGetValue(oldKey, out HashSet<RowKey>? set))
            {
                set.Remove(rowId);
                if (set.Count == 0)
                {
                    _rowsByKey.Remove(oldKey);
                }
            }
        }

        public IReadOnlyCollection<RowKey> Lookup(string key) =>
            _rowsByKey.TryGetValue(key, out HashSet<RowKey>? set) ? set : EmptyRowKeys;
    }

    internal sealed class TablePendingKeyState
    {
        public Dictionary<RowKey, Dictionary<string, object?>> CommittedRows { get; } = [];
        public HashSet<RowKey> KeyModifiedRowIds { get; } = [];
        public Parser.DML.InsertRowService.PendingKeys? CachedKeysWithInserts { get; set; }
        public int ProcessedInsertCount { get; set; }
        public int ProcessedUpdateCount { get; set; }
        public bool Dirty { get; set; } = true;
    }

    private sealed class TableColumnComparer : IEqualityComparer<(string TableName, string ColumnName)>
    {
        public static readonly TableColumnComparer Instance = new();

        public bool Equals((string TableName, string ColumnName) x, (string TableName, string ColumnName) y) =>
            StringComparer.OrdinalIgnoreCase.Equals(x.TableName, y.TableName)
            && StringComparer.OrdinalIgnoreCase.Equals(x.ColumnName, y.ColumnName);

        public int GetHashCode((string TableName, string ColumnName) obj) =>
            HashCode.Combine(
                StringComparer.OrdinalIgnoreCase.GetHashCode(obj.TableName),
                StringComparer.OrdinalIgnoreCase.GetHashCode(obj.ColumnName));
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
        if (!InsertedRows.TryGetValue(tableName, out List<Dictionary<string, object?>>? inserted))
        {
            inserted = [];
            InsertedRows[tableName] = inserted;
        }

        int position = inserted.Count;
        inserted.Add(row);
        UpdateBufferedColumnIndexes(tableName, BufferedRowKey(position), row);
    }

    /// <summary>
    /// Marks a row ID for deletion in the specified table.
    /// </summary>
    /// <param name="tableName">The target table name.</param>
    /// <param name="rowId">The physical row ID to delete.</param>
    public void BufferDelete(string tableName, RowKey rowId)
    {
        if (!DeletedRowIds.TryGetValue(tableName, out HashSet<RowKey>? deleted))
        {
            deleted = [];
            DeletedRowIds[tableName] = deleted;
        }

        deleted.Add(rowId);

        // An update buffered earlier for this row is void: COMMIT deletes the row, and a row inserted later
        // with the same key must not receive it.
        RemoveBufferedUpdate(tableName, rowId);
        RemoveFromBufferedColumnIndexes(tableName, rowId);
        InvalidatePendingKeys(tableName);
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
    /// <param name="keyColumnsUnchanged">
    /// <see langword="true"/> when the update statement leaves every primary-key and unique-key column unchanged.
    /// </param>
    public void BufferUpdate(string tableName, RowKey rowId, Dictionary<string, object?> updatedColumns, bool keyColumnsUnchanged = false)
    {
        if (!UpdatedRows.TryGetValue(tableName, out var updates))
        {
            updates = [];
            UpdatedRows[tableName] = updates;
        }

        if (!_updatedIndexByRowId.TryGetValue(tableName, out Dictionary<RowKey, int>? indexByRowId))
        {
            indexByRowId = [];
            _updatedIndexByRowId[tableName] = indexByRowId;
        }

        Dictionary<string, object?> currentRow;
        if (indexByRowId.TryGetValue(rowId, out int existingIndex))
        {
            currentRow = updates[existingIndex].UpdatedColumns;
            foreach (KeyValuePair<string, object?> column in updatedColumns)
            {
                currentRow[column.Key] = column.Value;
            }
        }
        else
        {
            indexByRowId[rowId] = updates.Count;
            updates.Add((rowId, updatedColumns));
            currentRow = updatedColumns;
        }

        UpdateBufferedColumnIndexes(tableName, rowId, currentRow);
        if (!keyColumnsUnchanged)
        {
            TablePendingKeyState state = GetOrCreatePendingKeyState(tableName);
            state.KeyModifiedRowIds.Add(rowId);
            state.Dirty = true;
        }
    }

    /// <summary>Replaces a buffered insert with its updated values (an UPDATE of a row this transaction inserted).</summary>
    /// <param name="tableName">The target table name.</param>
    /// <param name="rowId">The buffered row's reserved key (see <see cref="BufferedRowKey"/>).</param>
    /// <param name="row">The row's new values.</param>
    /// <param name="keyColumnsUnchanged">
    /// <see langword="true"/> when the update statement leaves every primary-key and unique-key column unchanged.
    /// </param>
    internal void ReplaceBufferedInsert(string tableName, RowKey rowId, Dictionary<string, object?> row, bool keyColumnsUnchanged = false)
    {
        List<Dictionary<string, object?>> rows = InsertedRows[tableName];
        Dictionary<string, object?> current = rows[(int)rowId.Low];
        foreach (KeyValuePair<string, object?> column in row)
        {
            current[column.Key] = column.Value;
        }

        UpdateBufferedColumnIndexes(tableName, rowId, current);
        if (!keyColumnsUnchanged)
        {
            InvalidatePendingKeys(tableName);
        }
    }

    /// <summary>
    /// Re-indexes a buffered row (an updated stored row or a buffered insert) after an UPDATE changed some of its
    /// non-key values in place. Equivalent to <see cref="BufferUpdate"/> or <see cref="ReplaceBufferedInsert"/>
    /// with every key column unchanged, without building a second copy of the row.
    /// </summary>
    /// <param name="tableName">The target table name.</param>
    /// <param name="rowId">The row's stored key, or its reserved buffered-insert key.</param>
    /// <param name="row">The buffered row whose values were changed.</param>
    internal void RefreshBufferedRow(string tableName, RowKey rowId, Dictionary<string, object?> row) =>
        UpdateBufferedColumnIndexes(tableName, rowId, row);

    /// <summary>Gets the buffered insert at a reserved buffered-row key, if it still exists.</summary>
    /// <param name="tableName">The target table name.</param>
    /// <param name="rowId">The reserved key (see <see cref="BufferedRowKey"/>).</param>
    /// <param name="row">The buffered row, when found.</param>
    /// <returns><see langword="true"/> when the buffered insert exists.</returns>
    internal bool TryGetBufferedInsert(string tableName, RowKey rowId, [NotNullWhen(true)] out Dictionary<string, object?>? row)
    {
        if (IsBufferedRowKey(rowId)
            && InsertedRows.TryGetValue(tableName, out List<Dictionary<string, object?>>? rows)
            && rowId.Low < (ulong)rows.Count)
        {
            row = rows[(int)rowId.Low];
            return true;
        }

        row = null;
        return false;
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

        bool removedAny = false;
        foreach (int position in rowIds.Select(rowId => (int)rowId.Low).Distinct().OrderByDescending(position => position))
        {
            if (position < rows.Count)
            {
                rows.RemoveAt(position);
                removedAny = true;
            }
        }

        if (removedAny)
        {
            InvalidateBufferedColumnIndexes(tableName);
            InvalidatePendingKeys(tableName);
        }
    }

    /// <summary>Gets whether the given stored row has been marked for deletion in this transaction.</summary>
    internal bool IsDeleted(string tableName, RowKey rowId) =>
        DeletedRowIds.TryGetValue(tableName, out HashSet<RowKey>? deleted) && deleted.Contains(rowId);

    /// <summary>
    /// Looks up the latest buffered update for <paramref name="rowId"/> in <paramref name="tableName"/> in O(1) time.
    /// </summary>
    internal bool TryGetUpdatedRow(string tableName, RowKey rowId, [NotNullWhen(true)] out Dictionary<string, object?>? updatedRow)
    {
        if (_updatedIndexByRowId.TryGetValue(tableName, out Dictionary<RowKey, int>? indexByRowId)
            && indexByRowId.TryGetValue(rowId, out int index)
            && UpdatedRows.TryGetValue(tableName, out var updates)
            && (uint)index < (uint)updates.Count)
        {
            updatedRow = updates[index].UpdatedColumns;
            return true;
        }

        updatedRow = null;
        return false;
    }

    /// <summary>
    /// Returns the buffered row identifiers (both updated stored rows and buffered inserts) whose current value for
    /// <paramref name="columnName"/> encodes to <paramref name="key"/>. Builds a per-column lookup table on first use
    /// and maintains it incrementally as subsequent writes are buffered.
    /// </summary>
    internal IReadOnlyCollection<RowKey> GetMatchingBufferedRowIds(string tableName, string columnName, string key)
    {
        var indexKey = (tableName, columnName);
        if (!_columnIndexes.TryGetValue(indexKey, out BufferedColumnIndex? columnIndex))
        {
            columnIndex = BuildBufferedColumnIndex(tableName, columnName);
            _columnIndexes[indexKey] = columnIndex;
        }

        return columnIndex.Lookup(key);
    }

    /// <summary>Gets or creates the cached pending-key state for <paramref name="tableName"/>.</summary>
    internal TablePendingKeyState GetOrCreatePendingKeyState(string tableName)
    {
        if (!_pendingKeysByTable.TryGetValue(tableName, out TablePendingKeyState? state))
        {
            state = new TablePendingKeyState();
            _pendingKeysByTable[tableName] = state;
        }

        return state;
    }

    /// <summary>Gets whether any buffered update on <paramref name="tableName"/> may have modified a key column.</summary>
    internal bool HasKeyModifyingUpdates(string tableName) =>
        _pendingKeysByTable.TryGetValue(tableName, out TablePendingKeyState? state)
        && state.KeyModifiedRowIds.Count > 0;

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
        _updatedIndexByRowId.Clear();
        _pendingKeysByTable.Clear();
        foreach ((string tableName, List<(RowKey RowId, Dictionary<string, object?> UpdatedColumns)> rows) in CloneUpdatedRows(snapshot.UpdatedRows))
        {
            UpdatedRows[tableName] = rows;
            var indexMap = new Dictionary<RowKey, int>(rows.Count);
            TablePendingKeyState pendingState = GetOrCreatePendingKeyState(tableName);
            for (int i = 0; i < rows.Count; i++)
            {
                indexMap[rows[i].RowId] = i;
                pendingState.KeyModifiedRowIds.Add(rows[i].RowId);
            }

            _updatedIndexByRowId[tableName] = indexMap;
        }

        _columnIndexes.Clear();
    }

    private void RemoveBufferedUpdate(string tableName, RowKey rowId)
    {
        if (!_updatedIndexByRowId.TryGetValue(tableName, out Dictionary<RowKey, int>? indexByRowId)
            || !indexByRowId.Remove(rowId, out int removedIndex)
            || !UpdatedRows.TryGetValue(tableName, out var updates))
        {
            return;
        }

        updates.RemoveAt(removedIndex);
        for (int i = removedIndex; i < updates.Count; i++)
        {
            indexByRowId[updates[i].RowId] = i;
        }
    }

    private BufferedColumnIndex BuildBufferedColumnIndex(string tableName, string columnName)
    {
        var index = new BufferedColumnIndex();
        DeletedRowIds.TryGetValue(tableName, out HashSet<RowKey>? deleted);

        if (UpdatedRows.TryGetValue(tableName, out var updates))
        {
            for (int i = 0; i < updates.Count; i++)
            {
                (RowKey rowId, Dictionary<string, object?> updatedRow) = updates[i];
                if (deleted?.Contains(rowId) != true)
                {
                    index.Upsert(rowId, updatedRow, columnName);
                }
            }
        }

        if (InsertedRows.TryGetValue(tableName, out List<Dictionary<string, object?>>? inserted))
        {
            for (int i = 0; i < inserted.Count; i++)
            {
                index.Upsert(BufferedRowKey(i), inserted[i], columnName);
            }
        }

        return index;
    }

    private void UpdateBufferedColumnIndexes(string tableName, RowKey rowId, IReadOnlyDictionary<string, object?> row)
    {
        if (_columnIndexes.Count == 0)
        {
            return;
        }

        foreach (KeyValuePair<(string TableName, string ColumnName), BufferedColumnIndex> entry in _columnIndexes)
        {
            if (string.Equals(entry.Key.TableName, tableName, StringComparison.OrdinalIgnoreCase))
            {
                entry.Value.Upsert(rowId, row, entry.Key.ColumnName);
            }
        }
    }

    private void RemoveFromBufferedColumnIndexes(string tableName, RowKey rowId)
    {
        if (_columnIndexes.Count == 0)
        {
            return;
        }

        foreach (KeyValuePair<(string TableName, string ColumnName), BufferedColumnIndex> entry in _columnIndexes)
        {
            if (string.Equals(entry.Key.TableName, tableName, StringComparison.OrdinalIgnoreCase))
            {
                entry.Value.Remove(rowId);
            }
        }
    }

    private void InvalidateBufferedColumnIndexes(string tableName)
    {
        if (_columnIndexes.Count == 0)
        {
            return;
        }

        List<(string TableName, string ColumnName)>? keysToRemove = null;
        foreach ((string TableName, string ColumnName) key in _columnIndexes.Keys)
        {
            if (string.Equals(key.TableName, tableName, StringComparison.OrdinalIgnoreCase))
            {
                keysToRemove ??= [];
                keysToRemove.Add(key);
            }
        }

        if (keysToRemove is not null)
        {
            for (int i = 0; i < keysToRemove.Count; i++)
            {
                _columnIndexes.Remove(keysToRemove[i]);
            }
        }
    }

    private void InvalidatePendingKeys(string tableName)
    {
        if (_pendingKeysByTable.TryGetValue(tableName, out TablePendingKeyState? state))
        {
            state.Dirty = true;
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
