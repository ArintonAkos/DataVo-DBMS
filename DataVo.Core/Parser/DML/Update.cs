using DataVo.Core.BTree;
using DataVo.Core.Cache;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Models.DML;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.Utils;
using DataVo.Core.StorageEngine;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.DML;

internal class Update(UpdateStatement ast) : BaseDbAction
{
    private readonly UpdateModel _model = UpdateModel.FromAst(ast);

    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = CacheStorage.Get(session)
                ?? throw new Exception("No database in use!");

            // 1. Identify which rows to update
            var whereStatement = new DataVo.Core.Parser.Statements.Where(_model.WhereExpression);
            List<long> toBeUpdated = whereStatement.EvaluateWithoutJoin(_model.TableName, databaseName).ToList();

            if (toBeUpdated.Count == 0)
            {
                Messages.Add("Rows affected: 0");
                return;
            }

            // 2. Fetch the existing rows from the storage
            var existingRows = StorageContext.Instance.GetTableContents(toBeUpdated, _model.TableName, databaseName);

            // 3. Prepare the new versions of the rows and fetch metadata
            List<Dictionary<string, dynamic>> newRows = new(existingRows.Count);
            List<long> oldRowIds = new(existingRows.Count);

            var primaryKeys = Catalog.GetTablePrimaryKeys(_model.TableName, databaseName);
            var uniqueKeys = Catalog.GetTableUniqueKeys(_model.TableName, databaseName);
            var foreignKeysList = Catalog.GetTableForeignKeys(_model.TableName, databaseName);
            var foreignKeys = foreignKeysList.ToDictionary(k => k.AttributeName);
            var childFks = Catalog.GetChildForeignKeys(_model.TableName, databaseName);
            var indexFiles = Catalog.GetTableIndexes(_model.TableName, databaseName);

            // To track uniqueness within the current update batch
            Dictionary<string, HashSet<string>> batchUniqueValues = new();
            foreach (var col in primaryKeys.Concat(uniqueKeys))
            {
                batchUniqueValues[col] = new HashSet<string>();
            }

            // 4. Validate constraints and evaluate SET expressions in memory
            int rowNumber = 1;
            foreach (var (rowId, oldRow) in existingRows)
            {
                oldRowIds.Add(rowId);

                // Copy old row
                var newRow = new Dictionary<string, dynamic>(oldRow);

                // Apply SET expressions
                foreach (var setExpr in _model.SetExpressions)
                {
                    string colName = setExpr.Key;
                    dynamic? newValue = ScalarEvaluator.Evaluate(setExpr.Value, oldRow);
                    if (newValue is string s && s.StartsWith("'") && s.EndsWith("'"))
                    {
                        newValue = s.Trim('\'');
                    }

                    if (newValue == null)
                        newRow[colName] = "null"; // TODO: True null support in Phase 1.2
                    else
                        newRow[colName] = newValue;
                }

                newRows.Add(newRow);

                // --- CONSTRAINT VERIFICATION ---

                // A. Primary Key / Unique Constraints
                foreach (var col in primaryKeys.Concat(uniqueKeys))
                {
                    if (newRow.TryGetValue(col, out var val))
                    {
                        string valStr = val?.ToString() ?? "null";

                        // If the value didn't change, it's fine
                        string oldValStr = oldRow.TryGetValue(col, out var oldVal) ? (oldVal?.ToString() ?? "null") : "null";
                        if (valStr == oldValStr) continue;

                        // Check within this batch
                        if (!batchUniqueValues[col].Add(valStr))
                        {
                            throw new Exception($"Update conflict: Duplicate value '{valStr}' generated within the same batch for unique column {col}.");
                        }

                        // Check DB index
                        string indexName = primaryKeys.Contains(col) ? $"_PK_{_model.TableName}" : $"_UK_{col}";
                        if (IndexManager.Instance.IndexContainsKey(valStr, indexName, _model.TableName, databaseName))
                        {
                            throw new Exception($"Constraint violation: Duplicate value '{valStr}' for unique column {col} in row {rowNumber}.");
                        }
                    }
                }

                // B. Foreign Key validation (us referencing a parent)
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

                // C. Child Foreign Key validation (us being referenced as a parent)
                // If we are changing a PK that is referenced by children, we must check ON DELETE/UPDATE rules.
                // Note: The Catalog currently parses ON DELETE, but standard SQL also has ON UPDATE.
                // For now, if the PK changes, we treat it like a RESTRICT if child rows exist.
                foreach (var childFk in childFks)
                {
                    string oldParentValStr = oldRow.TryGetValue(childFk.ParentColumn, out var oldPVal) ? (oldPVal?.ToString() ?? "null") : "null";
                    string newParentValStr = newRow.TryGetValue(childFk.ParentColumn, out var newPVal) ? (newPVal?.ToString() ?? "null") : "null";

                    if (oldParentValStr != newParentValStr && oldParentValStr != "null")
                    {
                        // The primary key has changed. Check if children exist referencing the old key.
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

                rowNumber++;
            }

            // 5. Execution (Delete old + Insert new)
            // 5a. Delete old records from storage & indexes
            StorageContext.Instance.DeleteFromTable(oldRowIds, _model.TableName, databaseName);
            foreach (var index in indexFiles)
            {
                IndexManager.Instance.DeleteFromIndex(oldRowIds, index.IndexFileName, _model.TableName, databaseName);
            }

            // 5b. Insert new records into storage & indexes
            for (int i = 0; i < newRows.Count; i++)
            {
                var newRow = newRows[i];
                long assignedRowId = StorageContext.Instance.InsertOneIntoTable(newRow, _model.TableName, databaseName);

                foreach (var index in indexFiles)
                {
                    string indexValue = IndexKeyEncoder.BuildKeyString(newRow, index.AttributeNames);
                    IndexManager.Instance.InsertIntoIndex(indexValue, assignedRowId, index.IndexFileName, _model.TableName, databaseName);
                }
            }

            Messages.Add($"Rows affected: {newRows.Count}");
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add(ex.Message);
        }
    }

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
