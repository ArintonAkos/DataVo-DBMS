using DataVo.Core.BTree;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser.Actions;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.Transactions;
using DataVo.Core.MVCC;
using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Parser.Transactions;

/// <summary>
/// Executes a <c>COMMIT</c> command by flushing all buffered DML operations
/// from the active <see cref="TransactionContext"/> to the physical storage engine.
/// <para>
/// Replays buffered inserts, deletes, and updates in order, including B-Tree index
/// maintenance, using the same <see cref="StorageContext"/> APIs that the standard
/// DML executors use.
/// </para>
/// </summary>
internal class Commit : BaseDbAction
{
    /// <summary>
    /// Retrieves the transaction context, then replays all buffered inserts, deletes,
    /// and updates against the storage engine in a single atomic flush.
    /// </summary>
    /// <param name="session">The unique session identifier.</param>
    public override void PerformAction(Guid session)
    {
        try
        {
            var context = Transactions.BeginCommit(session);
            try
            {
                Apply(context, GetDatabaseName(session));
            }
            finally
            {
                // The transaction ends here whether the commit succeeded or failed; only then may the versions its
                // conflict checks read be vacuumed.
                Transactions.EndCommit(session);
                MvccCoordinator.PruneRetainedImages(Engine);
            }

            Messages.Add("Transaction committed.");
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            AddError(ex);
        }
    }

    private void Apply(TransactionContext context, string databaseName)
    {
        // The commit stamp is taken before any lock: it fixes whether this commit records versions (and enters the
        // snapshot gate as a writer) before the commit can wait on anything.
        long commitStamp = Engine.CommitClock.AcquireStamp(context.TransactionId);

        var lockedResources = AcquireWriteLocks(databaseName, context);
        DataVoConfig config = Context.Config;
        bool walEnabled = config.StorageMode == StorageMode.Disk && config.WalEnabled;
        WalEntry? walEntry = walEnabled ? WalEntry.FromTransactionContext(databaseName, context) : null;
        WalWriter? walWriter = walEnabled ? new WalWriter(config) : null;

        try
        {
            // Every check that can reject the commit runs before anything is logged or applied, so a failed COMMIT
            // never reaches the WAL, storage, the indexes or the version metadata.
            ValidateBeforeApply(context, databaseName);

            AnnounceBufferedChanges(context, databaseName);

            if (walWriter != null && walEntry != null)
            {
                walWriter.Append(walEntry);
            }

            try
            {
                FlushContext(context, databaseName, Engine, commitStamp);
            }
            catch when (walWriter != null && walEntry != null)
            {
                // The flush was undone: retire the logged entry so recovery does not replay a rejected commit.
                walWriter.MarkCheckpointed(walEntry.TransactionId);
                throw;
            }

            if (walWriter != null && walEntry != null)
            {
                walWriter.MarkCheckpointed(walEntry.TransactionId);
            }
        }
        finally
        {
            ReleaseWriteLocks(databaseName, lockedResources);
        }
    }

    /// <summary>
    /// Rejects the commit before anything is applied: a buffered delete or update of a row another transaction
    /// changed since this one began (first committer wins), a buffered insert whose key another session committed
    /// meanwhile, or a buffered update whose new key value another session committed meanwhile.
    /// </summary>
    private void ValidateBeforeApply(TransactionContext context, string databaseName)
    {
        foreach (var (tableName, rowIds) in context.DeletedRowIds)
        {
            foreach (RowKey rowId in rowIds)
            {
                MvccCoordinator.ValidateCanModifyRow(Engine, databaseName, tableName, rowId, context.Snapshot, "DELETE");
            }
        }

        var service = new DML.InsertRowService(Engine, Context, Catalog, Indexes);
        foreach (var (tableName, updates) in context.UpdatedRows)
        {
            foreach (var (rowId, _) in updates)
            {
                MvccCoordinator.ValidateCanModifyRow(Engine, databaseName, tableName, rowId, context.Snapshot, "UPDATE");
            }

            ValidateUpdatedKeys(context, databaseName, tableName, updates);
        }

        foreach (var (tableName, rows) in context.InsertedRows)
        {
            service.ValidatePendingInserts(databaseName, tableName, rows, checkForeignKeys: false, owner: context);
        }
    }

    /// <summary>
    /// Checks that the new primary-key and unique values of a table's buffered updates are still free in committed
    /// storage. Every key a row this transaction updates or deletes held before the transaction counts as free — the
    /// commit removes all of those rows before it writes any new one — so swaps and rotations of keys between the
    /// transaction's own rows pass; two of its final rows with the same key still clash.
    /// </summary>
    private void ValidateUpdatedKeys(
        TransactionContext context,
        string databaseName,
        string tableName,
        List<(RowKey RowId, Dictionary<string, object?> UpdatedColumns)> updates)
    {
        if (!context.HasKeyModifyingUpdates(tableName))
        {
            return;
        }

        var primaryKeys = Catalog.GetTablePrimaryKeys(tableName, databaseName);
        var uniqueKeys = Catalog.GetTableUniqueKeys(tableName, databaseName);
        if (primaryKeys.Count == 0 && uniqueKeys.Count == 0)
        {
            return;
        }

        context.DeletedRowIds.TryGetValue(tableName, out HashSet<RowKey>? deleted);
        List<RowKey> touched = [.. updates.Select(update => update.RowId)];
        if (deleted is not null)
        {
            touched.AddRange(deleted);
        }

        Dictionary<RowKey, Dictionary<string, object?>> committed = Context.GetTableContents([.. touched.Distinct()], tableName, databaseName);
        var released = new DML.InsertRowService.PendingKeys();
        foreach (Dictionary<string, object?> original in committed.Values)
        {
            if (primaryKeys.Count > 0 && IndexKeyEncoder.HasCompleteKey(original, primaryKeys))
            {
                released.FreedPrimaryKeys.Add(IndexKeyEncoder.BuildKeyString(original, primaryKeys));
            }

            foreach (string column in uniqueKeys)
            {
                if (original.TryGetValue(column, out object? value) && value is not null)
                {
                    DML.InsertRowService.PendingKeys.Add(released.FreedUniqueValues, column, IndexKeyEncoder.BuildKeyString(original, [column]));
                }
            }
        }

        var validator = new DML.UpdateKeyValidator(Indexes, Context, tableName, databaseName, released);
        int rowNumber = 1;
        foreach (var (rowId, newRow) in updates)
        {
            if (deleted?.Contains(rowId) != true && committed.TryGetValue(rowId, out Dictionary<string, object?>? oldRow))
            {
                validator.Validate(newRow, oldRow, primaryKeys, uniqueKeys, rowNumber);
            }

            rowNumber++;
        }
    }

    /// <summary>
    /// Announces every buffered change before the transaction reaches the WAL or storage, so a subscriber's
    /// own log is always ahead of the commit. Crash recovery replays through the flush and announces nothing.
    /// </summary>
    private void AnnounceBufferedChanges(TransactionContext context, string databaseName)
    {
        if (ChangeRecorder.TryCreate(Engine, databaseName) is not { WantsAnnouncements: true } recorder)
        {
            return;
        }

        foreach (var (tableName, rows) in context.InsertedRows)
        {
            rows.ForEach(row => recorder.AnnounceInsert(tableName, row));
        }

        foreach (var (tableName, updates) in context.UpdatedRows)
        {
            var existing = Context.GetTableContents(updates.Select(update => update.RowId).Distinct().ToList(), tableName, databaseName);
            foreach (var (rowId, updatedColumns) in updates)
            {
                if (existing.TryGetValue(rowId, out var oldRow))
                {
                    var newRow = new Dictionary<string, object?>(oldRow, oldRow.Comparer);
                    foreach (var (column, value) in updatedColumns)
                    {
                        newRow[column] = value;
                    }

                    recorder.AnnounceUpdate(tableName, rowId, oldRow, newRow);
                }
            }
        }

        foreach (var (tableName, rowIds) in context.DeletedRowIds)
        {
            foreach (var (rowId, oldRow) in Context.GetTableContents(rowIds.ToList(), tableName, databaseName))
            {
                recorder.AnnounceDelete(tableName, rowId, oldRow);
            }
        }
    }

    /// <summary>
    /// Applies a transaction's buffered writes. Recovery replays logged transactions through the same path.
    /// </summary>
    /// <param name="context">The buffered writes.</param>
    /// <param name="databaseName">The owning database.</param>
    /// <param name="engine">The engine; the current engine when omitted.</param>
    /// <param name="commitStamp">The commit stamp; one is taken when omitted.</param>
    /// <remarks>
    /// The LSM backend applies the writes as one batch that is discarded on failure. The other backends write
    /// through immediately, so every change is journaled and undone if a later step throws: a failed commit leaves
    /// storage, indexes and version metadata as they were.
    /// </remarks>
    internal static void FlushContext(TransactionContext context, string databaseName, Runtime.DataVoEngine? engine = null, long? commitStamp = null)
    {
        var activeEngine = engine ?? Runtime.DataVoEngine.Current();
        using var _ = Runtime.DataVoEngine.PushCurrent(activeEngine);

        long txId = commitStamp ?? MvccCoordinator.ResolveStatementTransactionId(activeEngine, null);

        ChangeRecorder? recorder = ChangeRecorder.TryCreate(activeEngine, databaseName);
        var apply = new CommitApplier(activeEngine, databaseName, txId, recorder, journal: activeEngine.StorageContext.LsmEngine is null);

        try
        {
            // Deletes and updates name rows that existed before the transaction; inserts add new rows. Applying
            // the inserts last lets a transaction delete (or re-key) a row and insert its key again.
            apply.FlushDeletes(context);
            apply.FlushUpdates(context);
            apply.FlushInserts(context);
            apply.ValidateUpdatedForeignKeys();
        }
        catch
        {
            apply.Undo();
            throw;
        }

        recorder?.Publish();
    }

    private readonly record struct LockedResources(
        List<string> TableWriteLocks,
        Dictionary<string, List<RowKey>> RowWriteLocksByTable);

    private static LockedResources AcquireWriteLocks(string databaseName, TransactionContext context)
    {
        var lockManager = Runtime.DataVoEngine.Current().LockManager;

        var tableWriteLocks = context.InsertedRows.Keys
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(table => table, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var rowLockTables = context.DeletedRowIds.Keys
            .Concat(context.UpdatedRows.Keys)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(table => table, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var rowWriteLocksByTable = new Dictionary<string, List<RowKey>>(StringComparer.OrdinalIgnoreCase);

        foreach (string tableName in tableWriteLocks)
        {
            lockManager.AcquireWriteLock(databaseName, tableName);
        }

        foreach (string tableName in rowLockTables)
        {
            IEnumerable<RowKey> rowIds = Enumerable.Empty<RowKey>();
            if (context.DeletedRowIds.TryGetValue(tableName, out HashSet<RowKey>? deleted))
            {
                rowIds = rowIds.Concat(deleted);
            }

            if (context.UpdatedRows.TryGetValue(tableName, out List<(RowKey RowId, Dictionary<string, object?> UpdatedColumns)>? updated))
            {
                rowIds = rowIds.Concat(updated.Select(entry => entry.RowId));
            }

            List<RowKey> acquiredRowLocks = lockManager.AcquireRowWriteLocks(databaseName, tableName, rowIds);
            rowWriteLocksByTable[tableName] = acquiredRowLocks;
        }

        return new LockedResources(tableWriteLocks, rowWriteLocksByTable);
    }

    private static void ReleaseWriteLocks(string databaseName, LockedResources lockedResources)
    {
        var lockManager = Runtime.DataVoEngine.Current().LockManager;

        List<string> rowLockTables = [.. lockedResources.RowWriteLocksByTable.Keys
            .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)];

        for (int i = rowLockTables.Count - 1; i >= 0; i--)
        {
            string tableName = rowLockTables[i];

            if (lockedResources.RowWriteLocksByTable.TryGetValue(tableName, out List<RowKey>? rowLocks) && rowLocks.Count > 0)
            {
                lockManager.ReleaseRowWriteLocks(databaseName, tableName, rowLocks);
            }
        }

        for (int i = lockedResources.TableWriteLocks.Count - 1; i >= 0; i--)
        {
            string tableName = lockedResources.TableWriteLocks[i];
            lockManager.ReleaseWriteLock(databaseName, tableName);
        }
    }

    /// <summary>
    /// Applies one transaction's buffered writes with index and version maintenance, journaling each change so the
    /// whole flush can be undone when a step fails on a backend that writes through.
    /// </summary>
    private sealed class CommitApplier(Runtime.DataVoEngine engine, string databaseName, long txId, ChangeRecorder? recorder, bool journal)
    {
        private readonly List<(string Table, RowKey RowId, Dictionary<string, object?> Row, bool Inserted)> _steps = [];
        private readonly Dictionary<(string Table, RowKey RowId), RowVersion?> _versionsBefore = [];
        private readonly List<(string Table, Dictionary<string, object?> OldRow, Dictionary<string, object?> NewRow)> _updated = [];

        /// <summary>Applies the buffered deletes and purges their index entries.</summary>
        public void FlushDeletes(TransactionContext context)
        {
            foreach (var (tableName, rowIds) in context.DeletedRowIds)
            {
                var rowIdList = rowIds.ToList();
                foreach (RowKey rowId in rowIdList)
                {
                    MvccCoordinator.ValidateCanModifyRow(engine, databaseName, tableName, rowId, context.Snapshot, "DELETE");
                }

                var indexFiles = engine.Catalog.GetTableIndexes(tableName, databaseName);
                Dictionary<RowKey, Dictionary<string, object?>> oldRows = engine.StorageContext.GetTableContents(rowIdList, tableName, databaseName);

                foreach (RowKey rowId in rowIdList)
                {
                    RememberVersion(tableName, rowId);
                    MvccCoordinator.RegisterDeleteVersion(engine, databaseName, tableName, rowId, txId, oldRows.GetValueOrDefault(rowId));
                }

                if (recorder is { IsEnabled: true })
                {
                    foreach (RowKey rowId in rowIdList)
                    {
                        if (oldRows.TryGetValue(rowId, out var oldRow))
                        {
                            recorder.RecordDelete(tableName, rowId, oldRow);
                        }
                    }
                }

                engine.StorageContext.DeleteFromTable(rowIdList, tableName, databaseName);
                foreach (RowKey rowId in rowIdList)
                {
                    if (oldRows.TryGetValue(rowId, out var oldRow))
                    {
                        Journal(tableName, rowId, oldRow, inserted: false);
                    }
                }

                foreach (var index in indexFiles)
                {
                    RemoveIndexEntries(engine, index, rowIdList, oldRows, tableName, databaseName);
                }
            }
        }

        /// <summary>
        /// Applies the buffered updates out of place, in two phases per table: every old row (with its index entries)
        /// is removed before any new row is written. Row by row, a swap or rotation of key values among the
        /// transaction's rows would write a key while the row that still holds it was not yet removed.
        /// </summary>
        public void FlushUpdates(TransactionContext context)
        {
            foreach (var (tableName, updates) in context.UpdatedRows)
            {
                var indexFiles = engine.Catalog.GetTableIndexes(tableName, databaseName);
                var rows = new List<(RowKey RowId, Dictionary<string, object?> OldRow, Dictionary<string, object?> NewRow)>(updates.Count);

                // One read for the whole table's updated rows; reading has no side effects, so it can precede the
                // per-row conflict checks below, which still run (and throw) in buffer order.
                var rowIds = new List<RowKey>(updates.Count);
                foreach (var (rowId, _) in updates)
                {
                    rowIds.Add(rowId);
                }

                var existingRows = engine.StorageContext.GetTableContents(rowIds, tableName, databaseName);
                foreach (var (rowId, updatedColumns) in updates)
                {
                    MvccCoordinator.ValidateCanModifyRow(engine, databaseName, tableName, rowId, context.Snapshot, "UPDATE");
                    if (!existingRows.TryGetValue(rowId, out var oldRow)) continue;

                    var newRow = new Dictionary<string, object?>(oldRow, StringComparer.OrdinalIgnoreCase);
                    foreach (var (col, val) in updatedColumns)
                    {
                        newRow[col] = val;
                    }

                    rows.Add((rowId, oldRow, newRow));
                }

                bool modifiesKeys = context.HasKeyModifyingUpdates(tableName);
                bool isClustered = engine.StorageContext.ResolveClusteredKey(databaseName, tableName) is not null;

                // Phase 1: retire every old version (its image stays for older snapshots).
                var oldRows = new Dictionary<RowKey, Dictionary<string, object?>>(rows.Count);
                var oldRowsList = new List<Dictionary<string, object?>>(rows.Count);
                var newRowsList = new List<Dictionary<string, object?>>(rows.Count);
                foreach (var (rowId, oldRow, newRow) in rows)
                {
                    RememberVersion(tableName, rowId);
                    MvccCoordinator.RegisterDeleteVersion(engine, databaseName, tableName, rowId, txId, oldRow);
                    oldRows[rowId] = oldRow;
                    oldRowsList.Add(oldRow);
                    newRowsList.Add(newRow);
                }

                List<RowKey> oldRowIds = [.. oldRows.Keys];
                if (oldRowIds.Count > 0)
                {
                    if (isClustered || modifiesKeys)
                    {
                        engine.StorageContext.DeleteFromTable(oldRowIds, tableName, databaseName);
                    }

                    foreach (var (rowId, oldRow, _) in rows)
                    {
                        Journal(tableName, rowId, oldRow, inserted: false);
                    }

                    if (modifiesKeys)
                    {
                        foreach (var index in indexFiles)
                        {
                            RemoveIndexEntries(engine, index, oldRowIds, oldRows, tableName, databaseName);
                        }
                    }
                }

                // Phase 2: write every new row; a key an old row released may now be reused.
                var newRowIds = new List<RowKey>(rows.Count);
                foreach (var (_, oldRow, newRow) in rows)
                {
                    RowKey newRowId = engine.StorageContext.InsertOneIntoTable(newRow, tableName, databaseName);
                    newRowIds.Add(newRowId);
                    Journal(tableName, newRowId, newRow, inserted: true);
                    RememberVersion(tableName, newRowId);
                    MvccCoordinator.RegisterInsertVersion(engine, databaseName, tableName, newRowId, txId);
                    recorder?.RecordUpdate(tableName, newRowId, oldRow, newRow);
                    if (modifiesKeys)
                    {
                        AddIndexEntries(engine, indexFiles, newRow, newRowId, tableName, databaseName);
                    }

                    _updated.Add((tableName, oldRow, newRow));
                }

                if (!modifiesKeys && oldRowIds.Count > 0)
                {
                    foreach (var index in indexFiles)
                    {
                        CompiledQueries.DataVoCompiledQuery.MaintainIndexForUpdate(
                            engine,
                            tableName,
                            databaseName,
                            index,
                            oldRowIds,
                            oldRowsList,
                            newRowIds,
                            newRowsList);
                    }

                    if (!isClustered)
                    {
                        engine.StorageContext.DeleteFromTable(oldRowIds, tableName, databaseName);
                    }
                }
            }
        }

        /// <summary>
        /// Checks the foreign keys of updated rows once the whole commit is applied: a parent may be one this
        /// transaction inserts (applied after the updates), or one it — or another session meanwhile — deleted.
        /// </summary>
        public void ValidateUpdatedForeignKeys()
        {
            if (_updated.Count == 0)
            {
                return;
            }

            var validator = new DML.InsertRowService(engine, engine.StorageContext, engine.Catalog, engine.IndexManager);
            foreach (IGrouping<string, (string Table, Dictionary<string, object?> OldRow, Dictionary<string, object?> NewRow)> table in _updated.GroupBy(row => row.Table, StringComparer.OrdinalIgnoreCase))
            {
                validator.ValidateChangedForeignKeys(databaseName, table.Key, [.. table.Select(row => (row.OldRow, row.NewRow))]);
            }
        }

        /// <summary>Applies the buffered inserts, re-checking them against what other sessions committed.</summary>
        public void FlushInserts(TransactionContext context)
        {
            var validator = new DML.InsertRowService(engine, engine.StorageContext, engine.Catalog, engine.IndexManager);
            foreach (var (tableName, rows) in context.InsertedRows)
            {
                // The rows were validated when they were buffered and before the flush; this pass also checks foreign
                // keys, now that the parents this transaction inserted earlier are applied.
                validator.ValidatePendingInserts(databaseName, tableName, rows);

                var indexFiles = engine.Catalog.GetTableIndexes(tableName, databaseName);
                foreach (var row in rows)
                {
                    RowKey assignedRowId = engine.StorageContext.InsertOneIntoTable(row, tableName, databaseName);
                    Journal(tableName, assignedRowId, row, inserted: true);
                    RememberVersion(tableName, assignedRowId);
                    MvccCoordinator.RegisterInsertVersion(engine, databaseName, tableName, assignedRowId, txId);
                    recorder?.RecordInsert(tableName, assignedRowId, row);
                    AddIndexEntries(engine, indexFiles, row, assignedRowId, tableName, databaseName);
                }
            }
        }

        /// <summary>
        /// Reverses every journaled change, newest first: inserted rows and their index entries are removed, deleted
        /// rows are stored again with their index entries and version metadata, and the version metadata and retained
        /// images this commit recorded are restored. Nothing to do on the LSM backend, whose batch is discarded.
        /// </summary>
        public void Undo()
        {
            if (!journal)
            {
                return;
            }

            var restoredKeys = new Dictionary<(string Table, RowKey RowId), RowKey>();
            for (int i = _steps.Count - 1; i >= 0; i--)
            {
                (string table, RowKey rowId, Dictionary<string, object?> row, bool inserted) = _steps[i];
                var indexFiles = engine.Catalog.GetTableIndexes(table, databaseName);
                if (inserted)
                {
                    var rows = new Dictionary<RowKey, Dictionary<string, object?>> { [rowId] = row };
                    foreach (var index in indexFiles)
                    {
                        RemoveIndexEntries(engine, index, [rowId], rows, table, databaseName);
                    }

                    engine.StorageContext.DeleteFromTable([rowId], table, databaseName);
                }
                else
                {
                    RowKey restored = engine.StorageContext.InsertOneIntoTable(row, table, databaseName);
                    AddIndexEntries(engine, indexFiles, row, restored, table, databaseName);
                    restoredKeys[(table, rowId)] = restored;
                }
            }

            VersionStorageManager versions = engine.VersionStorageManager;
            foreach (((string table, RowKey rowId), RowVersion? before) in _versionsBefore)
            {
                RowKey key = restoredKeys.TryGetValue((table, rowId), out RowKey restored) ? restored : rowId;
                if (key != rowId)
                {
                    versions.RemoveVersion(databaseName, table, rowId);
                }

                if (before is { } version)
                {
                    versions.SetVersion(databaseName, table, key, version);
                }
                else
                {
                    versions.RemoveVersion(databaseName, table, key);
                }
            }

            versions.DiscardImages(txId);
        }

        private void Journal(string table, RowKey rowId, Dictionary<string, object?> row, bool inserted)
        {
            if (journal)
            {
                _steps.Add((table, rowId, row, inserted));
            }
        }

        private void RememberVersion(string table, RowKey rowId)
        {
            if (journal && !_versionsBefore.ContainsKey((table, rowId)))
            {
                _versionsBefore[(table, rowId)] = engine.VersionStorageManager.GetVersion(databaseName, table, rowId);
            }
        }
    }

    /// <summary>
    /// Adds a stored row's entries to every index of its table. A row whose key has a NULL column goes under the
    /// NULL key (see <see cref="Indexing.IndexManager.InsertNullKeyRow"/>); vector indexes take the row's vector.
    /// </summary>
    internal static void AddIndexEntries(
        Runtime.DataVoEngine engine,
        IReadOnlyList<IndexFile> indexFiles,
        Dictionary<string, object?> row,
        RowKey rowId,
        string tableName,
        string databaseName)
    {
        foreach (IndexFile index in indexFiles)
        {
            string indexKind = index.IndexKind ?? string.Empty;
            if (engine.IndexManager.SupportsVectorIndexType(indexKind))
            {
                if (index.AttributeNames.Count == 1
                    && row.TryGetValue(index.AttributeNames[0], out object? vectorValue)
                    && vectorValue is not null
                    && DataVo.Core.Utils.VectorParser.TryCoerceToVector(vectorValue, out float[] vector))
                {
                    engine.IndexManager.InsertIntoVectorIndex(vector, rowId, index.IndexFileName, tableName, databaseName, indexKind);
                }

                continue;
            }

            if (!IndexKeyEncoder.HasCompleteKey(row, index.AttributeNames))
            {
                engine.IndexManager.InsertNullKeyRow(rowId, index.IndexFileName, tableName, databaseName);
                continue;
            }

            string indexValue = IndexKeyEncoder.BuildKeyString(row, index.AttributeNames);
            engine.IndexManager.InsertIntoIndex(indexValue, rowId, index.IndexFileName, tableName, databaseName);
        }
    }

    private static void RemoveIndexEntries(
        Runtime.DataVoEngine engine,
        IndexFile index,
        List<RowKey> rowIds,
        IReadOnlyDictionary<RowKey, Dictionary<string, object?>> oldRows,
        string tableName,
        string databaseName)
    {
        string indexKind = index.IndexKind ?? string.Empty;
        if (engine.IndexManager.SupportsVectorIndexType(indexKind))
        {
            engine.IndexManager.DeleteFromVectorIndex(rowIds, index.IndexFileName, tableName, databaseName, indexKind);
            return;
        }

        engine.IndexManager.DeleteRowsFromIndex(index, rowIds, oldRows, tableName, databaseName);
    }
}
