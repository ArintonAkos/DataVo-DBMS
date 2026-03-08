using DataVo.Core.BTree;
using DataVo.Core.Cache;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser.Actions;
using DataVo.Core.StorageEngine;
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
            var context = TransactionManager.Instance.Commit(session);

            string databaseName = CacheStorage.Get(session)
                ?? throw new Exception("No database in use!");

            FlushInserts(context, databaseName);
            FlushDeletes(context, databaseName);
            FlushUpdates(context, databaseName);

            Messages.Add("Transaction committed.");
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add($"Error: {ex.Message}");
        }
    }

    /// <summary>
    /// Replays all buffered INSERT operations using <see cref="StorageContext.InsertOneIntoTable"/>
    /// and updates all associated B-Tree indexes.
    /// </summary>
    private static void FlushInserts(TransactionContext context, string databaseName)
    {
        foreach (var (tableName, rows) in context.InsertedRows)
        {
            var indexFiles = Catalog.GetTableIndexes(tableName, databaseName);

            foreach (var row in rows)
            {
                long assignedRowId = StorageContext.Instance.InsertOneIntoTable(row, tableName, databaseName);

                foreach (var index in indexFiles)
                {
                    if (index.AttributeNames.Any(attr => row.TryGetValue(attr, out var v) && v == null)) continue;

                    string indexValue = IndexKeyEncoder.BuildKeyString(row, index.AttributeNames);
                    IndexManager.Instance.InsertIntoIndex(indexValue, assignedRowId, index.IndexFileName, tableName, databaseName);
                }
            }
        }
    }

    /// <summary>
    /// Replays all buffered DELETE operations using <see cref="StorageContext.DeleteFromTable"/>
    /// and purges all associated B-Tree index entries.
    /// </summary>
    private static void FlushDeletes(TransactionContext context, string databaseName)
    {
        foreach (var (tableName, rowIds) in context.DeletedRowIds)
        {
            var rowIdList = rowIds.ToList();

            StorageContext.Instance.DeleteFromTable(rowIdList, tableName, databaseName);

            var indexFiles = Catalog.GetTableIndexes(tableName, databaseName);
            foreach (var index in indexFiles)
            {
                IndexManager.Instance.DeleteFromIndex(rowIdList, index.IndexFileName, tableName, databaseName);
            }
        }
    }

    /// <summary>
    /// Replays all buffered UPDATE operations using an out-of-place strategy (delete old + insert new),
    /// consistent with the standard <see cref="DML.Update"/> executor approach.
    /// </summary>
    private void FlushUpdates(TransactionContext context, string databaseName)
    {
        foreach (var (tableName, updates) in context.UpdatedRows)
        {
            var indexFiles = Catalog.GetTableIndexes(tableName, databaseName);

            foreach (var (rowId, updatedColumns) in updates)
            {
                // Retrieve the existing row, merge changes, and perform out-of-place update
                var existingRows = StorageContext.Instance.GetTableContents([rowId], tableName, databaseName);
                if (!existingRows.TryGetValue(rowId, out var oldRow)) continue;

                var newRow = new Dictionary<string, dynamic>(oldRow);
                foreach (var (col, val) in updatedColumns)
                {
                    newRow[col] = val!;
                }

                // Delete old + Insert new
                StorageContext.Instance.DeleteFromTable([rowId], tableName, databaseName);
                foreach (var index in indexFiles)
                {
                    IndexManager.Instance.DeleteFromIndex([rowId], index.IndexFileName, tableName, databaseName);
                }

                long newRowId = StorageContext.Instance.InsertOneIntoTable(newRow, tableName, databaseName);
                foreach (var index in indexFiles)
                {
                    if (index.AttributeNames.Any(attr => newRow[attr] == null)) continue;

                    string indexValue = IndexKeyEncoder.BuildKeyString(newRow, index.AttributeNames);
                    IndexManager.Instance.InsertIntoIndex(indexValue, newRowId, index.IndexFileName, tableName, databaseName);
                }
            }
        }
    }
}
