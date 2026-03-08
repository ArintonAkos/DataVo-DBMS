using DataVo.Core.BTree;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser.Actions;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.Transactions;

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
            var context = Transactions.Commit(session);

            string databaseName = GetDatabaseName(session);

            var lockedTables = AcquireWriteLocks(databaseName, context);
            DataVoConfig config = Context.Config;
            bool walEnabled = config.StorageMode == StorageMode.Disk && config.WalEnabled;
            WalEntry? walEntry = walEnabled ? WalEntry.FromTransactionContext(databaseName, context) : null;
            WalWriter? walWriter = walEnabled ? new WalWriter(config) : null;

            try
            {
                if (walWriter != null && walEntry != null)
                {
                    walWriter.Append(walEntry);
                }

                FlushContext(context, databaseName, Engine);

                if (walWriter != null && walEntry != null)
                {
                    walWriter.MarkCheckpointed(walEntry.TransactionId);
                }
            }
            finally
            {
                ReleaseWriteLocks(databaseName, lockedTables);
            }

            Messages.Add("Transaction committed.");
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add($"Error: {ex.Message}");
        }
    }

    internal static void FlushContext(TransactionContext context, string databaseName, Runtime.DataVoEngine? engine = null)
    {
        var activeEngine = engine ?? Runtime.DataVoEngine.Current();
        using var _ = Runtime.DataVoEngine.PushCurrent(activeEngine);

        FlushInserts(context, databaseName, activeEngine);
        FlushDeletes(context, databaseName, activeEngine);
        FlushUpdates(context, databaseName, activeEngine);
    }

    private static List<string> AcquireWriteLocks(string databaseName, TransactionContext context)
    {
        var tableNames = context.InsertedRows.Keys
            .Concat(context.DeletedRowIds.Keys)
            .Concat(context.UpdatedRows.Keys)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(table => table, StringComparer.OrdinalIgnoreCase)
            .ToList();

        foreach (string tableName in tableNames)
        {
            DataVo.Core.Runtime.DataVoEngine.Current().LockManager.AcquireWriteLock(databaseName, tableName);
        }

        return tableNames;
    }

    private static void ReleaseWriteLocks(string databaseName, List<string> tableNames)
    {
        for (int i = tableNames.Count - 1; i >= 0; i--)
        {
            DataVo.Core.Runtime.DataVoEngine.Current().LockManager.ReleaseWriteLock(databaseName, tableNames[i]);
        }
    }

    /// <summary>
    /// Replays all buffered INSERT operations using <see cref="StorageContext.InsertOneIntoTable"/>
    /// and updates all associated B-Tree indexes.
    /// </summary>
    private static void FlushInserts(TransactionContext context, string databaseName, Runtime.DataVoEngine engine)
    {
        foreach (var (tableName, rows) in context.InsertedRows)
        {
            var indexFiles = engine.Catalog.GetTableIndexes(tableName, databaseName);

            foreach (var row in rows)
            {
                long assignedRowId = engine.StorageContext.InsertOneIntoTable(row, tableName, databaseName);

                foreach (var index in indexFiles)
                {
                    if (index.AttributeNames.Any(attr => row.TryGetValue(attr, out var v) && v == null)) continue;

                    string indexValue = IndexKeyEncoder.BuildKeyString(row, index.AttributeNames);
                    engine.IndexManager.InsertIntoIndex(indexValue, assignedRowId, index.IndexFileName, tableName, databaseName);
                }
            }
        }
    }

    /// <summary>
    /// Replays all buffered DELETE operations using <see cref="StorageContext.DeleteFromTable"/>
    /// and purges all associated B-Tree index entries.
    /// </summary>
    private static void FlushDeletes(TransactionContext context, string databaseName, Runtime.DataVoEngine engine)
    {
        foreach (var (tableName, rowIds) in context.DeletedRowIds)
        {
            var rowIdList = rowIds.ToList();

            engine.StorageContext.DeleteFromTable(rowIdList, tableName, databaseName);

            var indexFiles = engine.Catalog.GetTableIndexes(tableName, databaseName);
            foreach (var index in indexFiles)
            {
                engine.IndexManager.DeleteFromIndex(rowIdList, index.IndexFileName, tableName, databaseName);
            }
        }
    }

    /// <summary>
    /// Replays all buffered UPDATE operations using an out-of-place strategy (delete old + insert new),
    /// consistent with the standard <see cref="DML.Update"/> executor approach.
    /// </summary>
    private static void FlushUpdates(TransactionContext context, string databaseName, Runtime.DataVoEngine engine)
    {
        foreach (var (tableName, updates) in context.UpdatedRows)
        {
            var indexFiles = engine.Catalog.GetTableIndexes(tableName, databaseName);

            foreach (var (rowId, updatedColumns) in updates)
            {
                // Retrieve the existing row, merge changes, and perform out-of-place update
                var existingRows = engine.StorageContext.GetTableContents([rowId], tableName, databaseName);
                if (!existingRows.TryGetValue(rowId, out var oldRow)) continue;

                var newRow = new Dictionary<string, dynamic>(oldRow);
                foreach (var (col, val) in updatedColumns)
                {
                    newRow[col] = val!;
                }

                // Delete old + Insert new
                engine.StorageContext.DeleteFromTable([rowId], tableName, databaseName);
                foreach (var index in indexFiles)
                {
                    engine.IndexManager.DeleteFromIndex([rowId], index.IndexFileName, tableName, databaseName);
                }

                long newRowId = engine.StorageContext.InsertOneIntoTable(newRow, tableName, databaseName);
                foreach (var index in indexFiles)
                {
                    if (index.AttributeNames.Any(attr => newRow[attr] == null)) continue;

                    string indexValue = IndexKeyEncoder.BuildKeyString(newRow, index.AttributeNames);
                    engine.IndexManager.InsertIntoIndex(indexValue, newRowId, index.IndexFileName, tableName, databaseName);
                }
            }
        }
    }
}
