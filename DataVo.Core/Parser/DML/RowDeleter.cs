using DataVo.Core.Exceptions;
using DataVo.Core.MVCC;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Changes;
using DataVo.Core.StorageEngine;
using DataVo.Core.Transactions;

namespace DataVo.Core.Parser.DML;

/// <summary>
/// The physical delete shared by SQL <c>DELETE</c> and <see cref="DataVoContext.ApplyBatch(DataVoWriteBatch)"/>: given row ids
/// that are already locked and validated, it removes them and keeps foreign keys, indexes, MVCC versions,
/// change capture and the write-ahead log consistent.
/// </summary>
internal static class RowDeleter
{
    /// <summary>
    /// Deletes stored rows with everything a delete must touch: foreign-key RESTRICT/CASCADE checks, MVCC
    /// delete versions, change capture, the table rows, every index and the implicit WAL commit.
    /// </summary>
    /// <param name="engine">The owning engine.</param>
    /// <param name="toBeDeleted">The stored row ids to delete; all must exist.</param>
    /// <param name="tableName">The table.</param>
    /// <param name="databaseName">The database.</param>
    /// <param name="statementTxId">The statement's MVCC transaction id.</param>
    /// <param name="recorder">The change recorder, or <see langword="null"/> when capture is off.</param>
    /// <param name="followForeignKeys">
    /// Whether to check RESTRICT and follow CASCADE for child tables. A caller that has already validated the
    /// whole delete set (cascades included) against its final state passes <see langword="false"/>.
    /// </param>
    /// <exception cref="EvaluationException">A RESTRICT foreign key still references a deleted row.</exception>
    /// <example>
    /// <code>
    /// RowDeleter.Delete(engine, [rowId], "Players", "Game", statementTxId, recorder);
    /// </code>
    /// </example>
    public static void Delete(DataVoEngine engine, List<RowKey> toBeDeleted, string tableName, string databaseName, long statementTxId, ChangeRecorder? recorder, bool followForeignKeys = true)
    {
        if (toBeDeleted.Count == 0) return;

        // FK enforcement: check child tables before deleting
        var childFks = followForeignKeys ? engine.Catalog.GetChildForeignKeys(tableName, databaseName) : [];

        if (childFks.Count > 0)
        {
            // Load the parent rows being deleted so we can check their FK column values
            var parentRows = engine.StorageContext.GetTableContents(toBeDeleted, tableName, databaseName);

            foreach (var childFk in childFks)
            {
                // For each parent row, check if any child row references it
                foreach (var (parentRowId, parentRow) in parentRows)
                {
                    if (!parentRow.TryGetValue(childFk.ParentColumn, out var parentValue) || parentValue == null)
                        continue;

                    string parentKeyStr = parentValue?.ToString() ?? "";

                    // Look up children via index scan on the child table
                    string childIndexName = $"_FK_{childFk.ChildTable}_{childFk.ChildColumn}";
                    List<RowKey> childRowIds;

                    try
                    {
                        childRowIds = engine.IndexManager.FilterUsingIndex(parentKeyStr, childIndexName, childFk.ChildTable, databaseName).ToList();
                    }
                    catch
                    {
                        // No FK index — fall back to full table scan
                        childRowIds = FindChildRowsByTableScan(engine, childFk.ChildTable, childFk.ChildColumn, parentKeyStr, databaseName);
                    }

                    // Filter out tombstoned rows
                    childRowIds = childRowIds
                        .Where(id => id != 0 && engine.StorageContext.TableContainsRow(id, childFk.ChildTable, databaseName))
                        .ToList();

                    if (childRowIds.Count == 0) continue;

                    if (childFk.OnDeleteAction == "RESTRICT")
                    {
                        throw new EvaluationException(
                            $"Foreign key violation: Cannot delete from {tableName} — " +
                            $"{childRowIds.Count} row(s) in {childFk.ChildTable}.{childFk.ChildColumn} " +
                            $"reference {childFk.ParentColumn} = {parentKeyStr}.");
                    }

                    if (childFk.OnDeleteAction == "CASCADE")
                    {
                        // Cascade delete: recursively clean up children -> grandchildren etc.
                        Delete(engine, childRowIds, childFk.ChildTable, databaseName, statementTxId, recorder);
                    }
                }
            }
        }

        // Old row images: reactive subscriptions need them as Before, and indexes use their keys to remove
        // exact entries instead of scanning every key for the row ids.
        List<Models.Catalog.IndexFile> indexFiles = engine.Catalog.GetTableIndexes(tableName, databaseName);
        Dictionary<RowKey, Dictionary<string, object?>>? deletedRows =
            recorder is { IsEnabled: true } || indexFiles.Count > 0 || MvccCoordinator.RecordsVersions(engine)
                ? engine.StorageContext.GetTableContents(toBeDeleted, tableName, databaseName)
                : null;

        foreach (RowKey rowId in toBeDeleted)
        {
            MvccCoordinator.RegisterDeleteVersion(engine, databaseName, tableName, rowId, statementTxId, deletedRows?.GetValueOrDefault(rowId));
        }

        if (recorder is { IsEnabled: true } && deletedRows is not null)
        {
            foreach (RowKey rowId in toBeDeleted)
            {
                if (deletedRows.TryGetValue(rowId, out var oldRow))
                {
                    recorder.AnnounceDelete(tableName, rowId, oldRow);
                    recorder.RecordDelete(tableName, rowId, oldRow);
                }
            }
        }

        // Delete entries from the main table
        engine.StorageContext.DeleteFromTable(toBeDeleted, tableName, databaseName);

        // Delete entries from all indexes
        foreach (Models.Catalog.IndexFile indexFile in indexFiles)
        {
            string indexKind = indexFile.IndexKind ?? string.Empty;
            if (engine.IndexManager.SupportsVectorIndexType(indexKind))
            {
                // Vector indexes are graphs, not key/row-id entries: the scalar removal below (and the LSM
                // index store) never reaches them, which left deleted rows in nearest-neighbour results.
                engine.IndexManager.DeleteFromVectorIndex(toBeDeleted, indexFile.IndexFileName, tableName, databaseName, indexKind);
                continue;
            }

            if (deletedRows is null)
            {
                engine.IndexManager.DeleteFromIndex(toBeDeleted, indexFile.IndexFileName, tableName, databaseName);
                continue;
            }

            var entries = new List<(string Key, RowKey RowId)>(toBeDeleted.Count);
            foreach (RowKey rowId in toBeDeleted)
            {
                if (!deletedRows.TryGetValue(rowId, out var oldRow))
                {
                    continue;
                }

                if (DataVo.Core.BTree.IndexKeyEncoder.HasCompleteKey(oldRow, indexFile.AttributeNames))
                {
                    entries.Add((DataVo.Core.BTree.IndexKeyEncoder.BuildKeyString(oldRow, indexFile.AttributeNames), rowId));
                }
                else
                {
                    engine.IndexManager.DeleteNullKeyRow(rowId, indexFile.IndexFileName, tableName, databaseName);
                }
            }

            engine.IndexManager.DeleteIndexEntries(entries, indexFile.IndexFileName, tableName, databaseName);
        }

        if (ImplicitWalCommit.IsEnabled(engine))
        {
            var operations = new List<WalOperation>(toBeDeleted.Count);
            foreach (RowKey rowId in toBeDeleted)
            {
                operations.Add(new WalOperation
                {
                    OperationType = WalOperationType.Delete,
                    TableName = tableName,
                    RowId = RowKeyConversions.ToRowId(rowId),
                });
            }

            ImplicitWalCommit.CommitIfEnabled(engine, databaseName, statementTxId, operations);
        }
    }

    /// <summary>
    /// Fallback for when no FK index exists — scans the child table for matching rows.
    /// </summary>
    private static List<RowKey> FindChildRowsByTableScan(DataVoEngine engine, string childTable, string childColumn, string parentValue, string databaseName)
    {
        var allRows = engine.StorageContext.GetTableContents(childTable, databaseName);
        var matchingIds = new List<RowKey>();

        foreach (var (rowId, row) in allRows)
        {
            if (row.TryGetValue(childColumn, out var val) && val != null && val?.ToString() == parentValue)
            {
                matchingIds.Add(rowId);
            }
        }

        return matchingIds;
    }
}
