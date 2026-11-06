using DataVo.Core.Logging;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Models.DML;
using DataVo.Core.Parser.Actions;
using DataVo.Core.BTree;
using DataVo.Core.Runtime;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Serialization;
using DataVo.Core.Parser.AST;
using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Transactions;
using DataVo.Core.MVCC;
using DataVo.Core.Runtime.Changes;

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
            long statementTxId = MvccCoordinator.ResolveStatementTransactionId(Engine, txContext?.TransactionId);
            if (txContext != null)
            {
                List<RowKey> toBeDeleted = _model.WhereStatement.EvaluateWithoutJoin(_model.TableName, databaseName).ToList();

                if (toBeDeleted.Count == 0)
                {
                    Messages.Add("Rows affected: 0");
                    return;
                }

                foreach (RowKey rowId in toBeDeleted)
                {
                    MvccCoordinator.ValidateCanModifyRow(Engine, databaseName, _model.TableName, rowId, txContext.Snapshot, "DELETE");
                    txContext.BufferDelete(_model.TableName, rowId);
                }

                Messages.Add($"Rows affected: {toBeDeleted.Count}");
            }
            else
            {
                List<RowKey> toBeDeleted = _model.WhereStatement.EvaluateWithoutJoin(_model.TableName, databaseName).ToList();

                if (toBeDeleted.Count == 0)
                {
                    Messages.Add("Rows affected: 0");
                    return;
                }

                List<RowKey> rowWriteLocks = Locks.AcquireRowWriteLocks(databaseName, _model.TableName, toBeDeleted);
                ChangeRecorder? recorder = ChangeRecorder.TryCreate(Engine, databaseName);
                try
                {
                    List<RowKey> revalidatedRowIds = RevalidateRowsAfterLock(databaseName, toBeDeleted, rowWriteLocks);
                    foreach (RowKey rowId in revalidatedRowIds)
                    {
                        MvccCoordinator.ValidateCanModifyRow(Engine, databaseName, _model.TableName, rowId, null, "DELETE");
                    }

                    ExecuteDelete(revalidatedRowIds, _model.TableName, databaseName, statementTxId, recorder);

                    Messages.Add($"Rows affected: {revalidatedRowIds.Count}");
                }
                finally
                {
                    Locks.ReleaseRowWriteLocks(databaseName, _model.TableName, rowWriteLocks);
                }

                recorder?.Publish();
            }
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            AddError(ex);
        }
    }

    private List<RowKey> RevalidateRowsAfterLock(
        string databaseName,
        IReadOnlyList<RowKey> originalCandidates,
        IReadOnlyList<RowKey> lockCoveredRowIds)
    {
        if (originalCandidates.Count == 0)
        {
            return [];
        }

        HashSet<RowKey> lockCovered = [.. lockCoveredRowIds];
        HashSet<RowKey> originalSet = [.. originalCandidates];

        ExpressionNode whereExpression = _model.WhereStatement.GetExpression()!;
        if (IsTriviallyTrueWhere(whereExpression))
        {
            return [.. originalCandidates];
        }

        return _model.WhereStatement.EvaluateWithoutJoin(_model.TableName, databaseName)
            .Where(rowId => originalSet.Contains(rowId))
            .Where(rowId => rowId <= 0 || lockCovered.Count == 0 || lockCovered.Contains(rowId))
            .ToList();
    }

    private static bool IsTriviallyTrueWhere(ExpressionNode expression)
    {
        if (expression is LiteralNode literal)
        {
            string? value = literal.Value?.ToString();
            return value != null && value.Equals("true", StringComparison.OrdinalIgnoreCase);
        }

        if (expression is ColumnRefNode columnRef)
        {
            return columnRef.Column.Equals("true", StringComparison.OrdinalIgnoreCase);
        }

        if (expression is ResolvedColumnRefNode resolved)
        {
            return resolved.Column.Equals("true", StringComparison.OrdinalIgnoreCase);
        }

        if (expression is BinaryExpressionNode binary
            && binary.Operator == "="
            && binary.Left is LiteralNode leftLiteral
            && binary.Right is LiteralNode rightLiteral)
        {
            return string.Equals(leftLiteral.Value?.ToString(), rightLiteral.Value?.ToString(), StringComparison.Ordinal);
        }

        return false;
    }

    private void ExecuteDelete(List<RowKey> toBeDeleted, string tableName, string databaseName, long statementTxId, ChangeRecorder? recorder)
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
                    List<RowKey> childRowIds;

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
                        throw new EvaluationException(
                            $"Foreign key violation: Cannot delete from {tableName} — " +
                            $"{childRowIds.Count} row(s) in {childFk.ChildTable}.{childFk.ChildColumn} " +
                            $"reference {childFk.ParentColumn} = {parentKeyStr}.");
                    }

                    if (childFk.OnDeleteAction == "CASCADE")
                    {
                        // Cascade delete: recursively clean up children -> grandchildren etc.
                        ExecuteDelete(childRowIds, childFk.ChildTable, databaseName, statementTxId, recorder);
                    }
                }
            }
        }

        foreach (RowKey rowId in toBeDeleted)
        {
            MvccCoordinator.RegisterDeleteVersion(Engine, databaseName, tableName, rowId, statementTxId);
        }

        // Old row images: reactive subscriptions need them as Before, and indexes use their keys to remove
        // exact entries instead of scanning every key for the row ids.
        List<Models.Catalog.IndexFile> indexFiles = Catalog.GetTableIndexes(tableName, databaseName);
        Dictionary<RowKey, Dictionary<string, object?>>? deletedRows =
            recorder is { IsEnabled: true } || indexFiles.Count > 0
                ? Context.GetTableContents(toBeDeleted, tableName, databaseName)
                : null;

        if (recorder is { IsEnabled: true } && deletedRows is not null)
        {
            foreach (RowKey rowId in toBeDeleted)
            {
                if (deletedRows.TryGetValue(rowId, out var oldRow))
                {
                    recorder.RecordDelete(tableName, rowId, oldRow);
                }
            }
        }

        // Delete entries from the main table
        Context.DeleteFromTable(toBeDeleted, tableName, databaseName);

        // Delete entries from all indexes
        foreach (Models.Catalog.IndexFile indexFile in indexFiles)
        {
            if (Indexes.SupportsVectorIndexType(indexFile.IndexKind ?? string.Empty) || deletedRows is null)
            {
                Indexes.DeleteFromIndex(toBeDeleted, indexFile.IndexFileName, tableName, databaseName);
                continue;
            }

            var entries = new List<(string Key, RowKey RowId)>(toBeDeleted.Count);
            foreach (RowKey rowId in toBeDeleted)
            {
                if (deletedRows.TryGetValue(rowId, out var oldRow) && indexFile.AttributeNames.All(oldRow.ContainsKey))
                {
                    entries.Add((DataVo.Core.BTree.IndexKeyEncoder.BuildKeyString(oldRow, indexFile.AttributeNames), rowId));
                }
            }

            Indexes.DeleteIndexEntries(entries, indexFile.IndexFileName, tableName, databaseName);
        }

        if (ImplicitWalCommit.IsEnabled(Engine))
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

            ImplicitWalCommit.CommitIfEnabled(Engine, databaseName, statementTxId, operations);
        }
    }

    /// <summary>
    /// Fallback for when no FK index exists — scans the child table for matching rows.
    /// </summary>
    private static List<RowKey> FindChildRowsByTableScan(string childTable, string childColumn, string parentValue, string databaseName)
    {
        var allRows = DataVoEngine.Current().StorageContext.GetTableContents(childTable, databaseName);
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
