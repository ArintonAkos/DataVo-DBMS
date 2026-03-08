using DataVo.Core.BTree;
using DataVo.Core.Cache;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Models.DML;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.Utils;
using DataVo.Core.StorageEngine;
using DataVo.Core.Parser.AST;
using DataVo.Core.Transactions;

namespace DataVo.Core.Parser.DML;

/// <summary>
/// Executes the SQL UPDATE command to modify existing records in a table based on a specified condition.
/// </summary>
/// <example>
/// <code>
/// // Example SQL: UPDATE Users SET Status = 'Active' WHERE Id = 1;
/// var updateAction = new Update(astNode);
/// updateAction.PerformAction(sessionId);
/// </code>
/// </example>
internal class Update(UpdateStatement ast) : BaseDbAction
{
    private readonly UpdateModel _model = UpdateModel.FromAst(ast);

    /// <summary>
    /// Evaluates the conditions to find matching rows, applies the SET expressions, validates constraints,
    /// and performs an out-of-place update (Delete + Insert) via the storage engine.
    /// </summary>
    /// <param name="session">The current user session ID.</param>
    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = CacheStorage.Get(session)
                ?? throw new Exception("No database in use!");

            var txContext = TransactionManager.Instance.GetContext(session);
            if (txContext != null)
            {
                List<long> toBeUpdated = IdentifyRowsToUpdate(databaseName);
                if (toBeUpdated.Count == 0)
                {
                    Messages.Add("Rows affected: 0");
                    return;
                }

                var existingRows = StorageContext.Instance.GetTableContents(toBeUpdated, _model.TableName, databaseName);

                (List<Dictionary<string, dynamic>> newRows, List<long> oldRowIds) = EvaluateAndVerifyConstraints(existingRows, databaseName);

                for (int i = 0; i < newRows.Count; i++)
                {
                    txContext.BufferUpdate(_model.TableName, oldRowIds[i], newRows[i]);
                }

                Messages.Add($"Rows affected: {newRows.Count}");
            }
            else
            {
                LockManager.Instance.AcquireWriteLock(databaseName, _model.TableName);

                try
                {
                    List<long> toBeUpdated = IdentifyRowsToUpdate(databaseName);
                    if (toBeUpdated.Count == 0)
                    {
                        Messages.Add("Rows affected: 0");
                        return;
                    }

                    var existingRows = StorageContext.Instance.GetTableContents(toBeUpdated, _model.TableName, databaseName);

                    (List<Dictionary<string, dynamic>> newRows, List<long> oldRowIds) = EvaluateAndVerifyConstraints(existingRows, databaseName);

                    ExecuteUpdate(newRows, oldRowIds, databaseName);

                    Messages.Add($"Rows affected: {newRows.Count}");
                }
                finally
                {
                    LockManager.Instance.ReleaseWriteLock(databaseName, _model.TableName);
                }
            }
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add(ex.Message);
        }
    }

    /// <summary>
    /// Determines which row IDs need to be updated based on the WHERE expression.
    /// </summary>
    /// <param name="databaseName">The active database name.</param>
    /// <returns>A list of matching row IDs.</returns>
    private List<long> IdentifyRowsToUpdate(string databaseName)
    {
        var whereStatement = new DataVo.Core.Parser.Statements.Where(_model.WhereExpression);
        return whereStatement.EvaluateWithoutJoin(_model.TableName, databaseName).ToList();
    }

    /// <summary>
    /// Evaluates SET expressions to produce new rows and validates all integrity constraints (PKs, UKs, FKs).
    /// </summary>
    /// <param name="existingRows">Dictionary of existing row IDs mapping to old row data.</param>
    /// <param name="databaseName">The active database name.</param>
    /// <returns>A tuple containing the newly evaluated rows and the corresponding old row IDs.</returns>
    private (List<Dictionary<string, dynamic>> NewRows, List<long> OldRowIds) EvaluateAndVerifyConstraints(
        Dictionary<long, Dictionary<string, dynamic>> existingRows,
        string databaseName)
    {
        List<Dictionary<string, dynamic>> newRows = new(existingRows.Count);
        List<long> oldRowIds = new(existingRows.Count);

        var primaryKeys = Catalog.GetTablePrimaryKeys(_model.TableName, databaseName);
        var uniqueKeys = Catalog.GetTableUniqueKeys(_model.TableName, databaseName);
        var foreignKeysList = Catalog.GetTableForeignKeys(_model.TableName, databaseName);
        var childFks = Catalog.GetChildForeignKeys(_model.TableName, databaseName);

        Dictionary<string, HashSet<string>> batchUniqueValues = InitializeBatchUniqueTracker(primaryKeys, uniqueKeys);

        int rowNumber = 1;
        foreach (var (rowId, oldRow) in existingRows)
        {
            oldRowIds.Add(rowId);

            // Create and add new row to the list of new rows
            var newRow = ApplySetExpressions(oldRow);
            newRows.Add(newRow);

            // Validate all constraints
            ValidatePrimaryAndUniqueConstraints(newRow, oldRow, primaryKeys, uniqueKeys, batchUniqueValues, rowNumber, databaseName);
            ValidateForeignKeyConstraints(newRow, oldRow, foreignKeysList, rowNumber, databaseName);
            ValidateChildForeignKeyConstraints(newRow, oldRow, childFks, databaseName);

            rowNumber++;
        }

        return (newRows, oldRowIds);
    }

    /// <summary>
    /// Initializes a tracker to prevent unique constraint violations within the selfsame bulk update batch.
    /// </summary>
    private static Dictionary<string, HashSet<string>> InitializeBatchUniqueTracker(List<string> primaryKeys, List<string> uniqueKeys)
    {
        Dictionary<string, HashSet<string>> batchUniqueValues = new();
        foreach (var col in primaryKeys.Concat(uniqueKeys))
        {
            batchUniqueValues[col] = new HashSet<string>();
        }
        return batchUniqueValues;
    }

    /// <summary>
    /// Applies the SQL SET expressions to the old row to calculate its new modified state.
    /// </summary>
    /// <param name="oldRow">The original row record.</param>
    /// <returns>A new dictionary representing the modified row.</returns>
    private Dictionary<string, dynamic> ApplySetExpressions(Dictionary<string, dynamic> oldRow)
    {
        var newRow = new Dictionary<string, dynamic>(oldRow);

        foreach (var setExpr in _model.SetExpressions)
        {
            string colName = setExpr.Key;
            dynamic? newValue = ScalarEvaluator.Evaluate(setExpr.Value, oldRow);

            if (newValue is string s && s.StartsWith("'") && s.EndsWith("'"))
            {
                newValue = s.Trim('\'');
            }

            newRow[colName] = newValue!;
        }

        return newRow;
    }

    /// <summary>
    /// Validates that the newly assigned values do not violate Primary Key or Unique Key constraints.
    /// </summary>
    private void ValidatePrimaryAndUniqueConstraints(
        Dictionary<string, dynamic> newRow,
        Dictionary<string, dynamic> oldRow,
        List<string> primaryKeys,
        List<string> uniqueKeys,
        Dictionary<string, HashSet<string>> batchUniqueValues,
        int rowNumber,
        string databaseName)
    {
        foreach (var col in primaryKeys.Concat(uniqueKeys))
        {
            if (newRow.TryGetValue(col, out var val))
            {
                if (val == null && primaryKeys.Contains(col))
                {
                    throw new Exception($"Constraint violation: Primary key column {col} cannot be null in row {rowNumber}.");
                }

                if (val == null) continue; // Unique keys can be null

                string valStr = val.ToString()!;
                string oldValStr = oldRow.TryGetValue(col, out var oldVal) ? (oldVal?.ToString() ?? "null_val") : "null_val";

                // Fine if it hasn't actually mutated
                if (valStr == oldValStr) continue;

                if (!batchUniqueValues[col].Add(valStr))
                {
                    throw new Exception($"Update conflict: Duplicate value '{valStr}' generated within the same batch for unique column {col}.");
                }

                string indexName = primaryKeys.Contains(col) ? $"_PK_{_model.TableName}" : $"_UK_{col}";
                if (IndexManager.Instance.IndexContainsKey(valStr, indexName, _model.TableName, databaseName))
                {
                    throw new Exception($"Constraint violation: Duplicate value '{valStr}' for unique column {col} in row {rowNumber}.");
                }
            }
        }
    }

    /// <summary>
    /// Validates that any updated foreign key column correctly references an existing parent Primary Key.
    /// </summary>
    private static void ValidateForeignKeyConstraints(
        Dictionary<string, dynamic> newRow,
        Dictionary<string, dynamic> oldRow,
        List<ForeignKey> foreignKeysList,
        int rowNumber,
        string databaseName)
    {
        foreach (var fk in foreignKeysList)
        {
            if (newRow.TryGetValue(fk.AttributeName, out var fkVal))
            {
                string fkValStr = fkVal?.ToString() ?? "null";
                string oldFkValStr = oldRow.TryGetValue(fk.AttributeName, out var oldFkVal) ? (oldFkVal?.ToString() ?? "null") : "null";

                if (fkValStr != oldFkValStr && fkValStr != "null")
                {
                    if (!CheckForeignKeyConstraint(fk, fkValStr, databaseName))
                    {
                        throw new Exception($"Foreign key violation: Value '{fkValStr}' perfectly validates against references in row {rowNumber}.");
                    }
                }
            }
        }
    }

    /// <summary>
    /// Validates that if a Primary Key is modified, it does not orphans records in child tables (RESTRICT enforcement).
    /// </summary>
    private void ValidateChildForeignKeyConstraints(
        Dictionary<string, dynamic> newRow,
        Dictionary<string, dynamic> oldRow,
        List<(string ChildTable, string ChildColumn, string ParentColumn, string OnDeleteAction)> childFks,
        string databaseName)
    {
        foreach (var childFk in childFks)
        {
            string oldParentValStr = oldRow.TryGetValue(childFk.ParentColumn, out var oldPVal) ? (oldPVal?.ToString() ?? "null") : "null";
            string newParentValStr = newRow.TryGetValue(childFk.ParentColumn, out var newPVal) ? (newPVal?.ToString() ?? "null") : "null";

            // If PK mutated safely
            if (oldParentValStr != newParentValStr && oldParentValStr != "null")
            {
                string childIndexName = $"_FK_{childFk.ChildTable}_{childFk.ChildColumn}";
                List<long> childRowIds;

                try
                {
                    childRowIds = IndexManager.Instance.FilterUsingIndex(oldParentValStr, childIndexName, childFk.ChildTable, databaseName).ToList();
                }
                catch
                {
                    childRowIds = FindChildRowsByTableScan(childFk.ChildTable, childFk.ChildColumn, oldParentValStr, databaseName);
                }

                childRowIds = childRowIds
                    .Where(id => id != 0 && StorageContext.Instance.TableContainsRow(id, childFk.ChildTable, databaseName))
                    .ToList();

                if (childRowIds.Count > 0)
                {
                    throw new Exception($"Foreign key violation: Cannot update {childFk.ParentColumn} ('{oldParentValStr}' -> '{newParentValStr}') in {_model.TableName} because {childRowIds.Count} row(s) in {childFk.ChildTable} depend on it.");
                }
            }
        }
    }

    /// <summary>
    /// Executes the update via the storage engine utilizing an out-of-place update architecture:
    /// Old records are deleted, and new records are inserted with identical RowIds or newly allocated ones.
    /// Index structures are purged and updated as well.
    /// </summary>
    private void ExecuteUpdate(List<Dictionary<string, dynamic>> newRows, List<long> oldRowIds, string databaseName)
    {
        var indexFiles = Catalog.GetTableIndexes(_model.TableName, databaseName);

        // Delete old records from storage & indexes
        StorageContext.Instance.DeleteFromTable(oldRowIds, _model.TableName, databaseName);
        foreach (var index in indexFiles)
        {
            IndexManager.Instance.DeleteFromIndex(oldRowIds, index.IndexFileName, _model.TableName, databaseName);
        }

        // Insert new records into storage & indexes
        for (int i = 0; i < newRows.Count; i++)
        {
            var newRow = newRows[i];
            long assignedRowId = StorageContext.Instance.InsertOneIntoTable(newRow, _model.TableName, databaseName);

            foreach (var index in indexFiles)
            {
                if (index.AttributeNames.Any(attr => newRow[attr] == null)) continue;

                string indexValue = IndexKeyEncoder.BuildKeyString(newRow, index.AttributeNames);
                IndexManager.Instance.InsertIntoIndex(indexValue, assignedRowId, index.IndexFileName, _model.TableName, databaseName);
            }
        }
    }

    /// <summary>
    /// Queries the IndexManager to determine if a required foreign key value exists in the parent's Primary Key index.
    /// </summary>
    /// <returns>True if the reference exists; otherwise false.</returns>
    private static bool CheckForeignKeyConstraint(ForeignKey foreignKey, string columnValue, string databaseName)
    {
        foreach (var reference in foreignKey.References)
        {
            if (!IndexManager.Instance.IndexContainsKey(columnValue, $"_PK_{reference.ReferenceTableName}", reference.ReferenceTableName, databaseName))
            {
                return false;
            }
        }
        return true;
    }

    /// <summary>
    /// Falls back to a linear scan to find dependent child rows if the child table lacks an index on the foreign key column.
    /// </summary>
    /// <returns>A list of Row IDs representing matched child records.</returns>
    private static List<long> FindChildRowsByTableScan(string childTable, string childColumn, string parentValue, string databaseName)
    {
        var allRows = StorageContext.Instance.GetTableContents(childTable, databaseName);
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
