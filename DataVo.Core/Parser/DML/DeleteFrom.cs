using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Models.DML;
using DataVo.Core.Parser.Actions;
using DataVo.Core.BTree;
using DataVo.Core.Runtime;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Serialization;
using DataVo.Core.Parser.AST;
using DataVo.Core.Transactions;

namespace DataVo.Core.Parser.DML;

internal class DeleteFrom(DeleteFromStatement ast) : BaseDbAction
{
    private readonly DeleteFromModel _model = DeleteFromModel.FromAst(ast);

    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = GetDatabaseName(session);

            var txContext = Transactions.GetContext(session);
            if (txContext != null)
            {
                List<long> toBeDeleted = _model.WhereStatement.EvaluateWithoutJoin(_model.TableName, databaseName).ToList();

                if (toBeDeleted.Count == 0)
                {
                    Messages.Add("Rows affected: 0");
                    return;
                }

                foreach (long rowId in toBeDeleted)
                {
                    txContext.BufferDelete(_model.TableName, rowId);
                }

                Messages.Add($"Rows affected: {toBeDeleted.Count}");
            }
            else
            {
                Locks.AcquireWriteLock(databaseName, _model.TableName);

                try
                {
                    List<long> toBeDeleted = _model.WhereStatement.EvaluateWithoutJoin(_model.TableName, databaseName).ToList();

                    if (toBeDeleted.Count == 0)
                    {
                        Messages.Add("Rows affected: 0");
                        return;
                    }

                    ExecuteDelete(toBeDeleted, _model.TableName, databaseName);
                    Messages.Add($"Rows affected: {toBeDeleted.Count}");
                }
                finally
                {
                    Locks.ReleaseWriteLock(databaseName, _model.TableName);
                }
            }
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add(ex.Message);
        }
    }

    private void ExecuteDelete(List<long> toBeDeleted, string tableName, string databaseName)
    {
        if (toBeDeleted.Count == 0) return;

        // FK enforcement: check child tables before deleting
        var childFks = Catalog.GetChildForeignKeys(tableName, databaseName);

        if (childFks.Count > 0)
        {
            // Load the parent rows being deleted so we can check their FK column values
            var parentRows = Context.GetTableContents(toBeDeleted, tableName, databaseName);

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
                    List<long> childRowIds;

                    try
                    {
                        childRowIds = Indexes.FilterUsingIndex(parentKeyStr, childIndexName, childFk.ChildTable, databaseName).ToList();
                    }
                    catch
                    {
                        // No FK index — fall back to full table scan
                        childRowIds = FindChildRowsByTableScan(childFk.ChildTable, childFk.ChildColumn, parentKeyStr, databaseName);
                    }

                    // Filter out tombstoned rows
                    childRowIds = childRowIds
                        .Where(id => id != 0 && Context.TableContainsRow(id, childFk.ChildTable, databaseName))
                        .ToList();

                    if (childRowIds.Count == 0) continue;

                    if (childFk.OnDeleteAction == "RESTRICT")
                    {
                        throw new Exception(
                            $"Foreign key violation: Cannot delete from {tableName} — " +
                            $"{childRowIds.Count} row(s) in {childFk.ChildTable}.{childFk.ChildColumn} " +
                            $"reference {childFk.ParentColumn} = {parentKeyStr}.");
                    }

                    if (childFk.OnDeleteAction == "CASCADE")
                    {
                        // Cascade delete: recursively clean up children -> grandchildren etc.
                        ExecuteDelete(childRowIds, childFk.ChildTable, databaseName);
                    }
                }
            }
        }

        // Delete entries from the main table
        Context.DeleteFromTable(toBeDeleted, tableName, databaseName);

        // Delete entries from all indexes
        Catalog.GetTableIndexes(tableName, databaseName)
            .Select(e => e.IndexFileName)
            .ToList()
            .ForEach(indexFile =>
            {
                Indexes.DeleteFromIndex(toBeDeleted, indexFile, tableName, databaseName);
            });
    }

    /// <summary>
    /// Fallback for when no FK index exists — scans the child table for matching rows.
    /// </summary>
    private static List<long> FindChildRowsByTableScan(string childTable, string childColumn, string parentValue, string databaseName)
    {
        var allRows = DataVoEngine.Current().StorageContext.GetTableContents(childTable, databaseName);
        var matchingIds = new List<long>();

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