using DataVo.Core.BTree;
using DataVo.Core.Exceptions;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Models.DML;
using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.Utils;
using DataVo.Core.Runtime;
using DataVo.Core.StorageEngine;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Statements.Mechanism;
using DataVo.Core.Services;
using DataVo.Core.Transactions;
using DataVo.Core.Utils;
using DataVo.Core.MVCC;
using DataVo.Core.Runtime.Changes;

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
    private Dictionary<string, ExpressionNode>? _materializedSetExpressions;

    /// <summary>
    /// Evaluates the conditions to find matching rows, applies the SET expressions, validates constraints,
    /// and performs an out-of-place update (Delete + Insert) via the storage engine.
    /// </summary>
    /// <param name="session">The current user session ID.</param>
    /// <summary>
    /// Rejects SET clauses that name columns the table does not have, before any row is read or written.
    /// </summary>
    private void ValidateAssignedColumns(string databaseName)
    {
        IReadOnlyList<Models.Catalog.Column> columns = Catalog.GetTableColumnsReadOnly(_model.TableName, databaseName);
        foreach (string assigned in _model.SetExpressions.Keys)
        {
            if (!HasColumn(columns, assigned))
            {
                throw new BindingException($"Column {assigned} doesn't exist in table {_model.TableName}.")
                {
                    ErrorCode = Contracts.Results.DataVoErrorCodes.UnknownColumn,
                };
            }
        }
    }

    private static bool HasColumn(IReadOnlyList<Models.Catalog.Column> columns, string name)
    {
        foreach (Models.Catalog.Column column in columns)
        {
            if (string.Equals(column.Name, name, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = GetDatabaseName(session);
            ValidateAssignedColumns(databaseName);
            _ = GetMaterializedSetExpressions(databaseName);

            var txContext = Transactions.GetContext(session);
            long statementTxId = MvccCoordinator.ResolveStatementTransactionId(Engine, txContext?.TransactionId);
            if (txContext != null)
            {
                List<RowKey> toBeUpdated = IdentifyRowsToUpdate(databaseName);
                if (toBeUpdated.Count == 0)
                {
                    Messages.Add("Rows affected: 0");
                    return;
                }

                var existingRows = Context.GetTableContents(toBeUpdated, _model.TableName, databaseName);

                foreach (RowKey rowId in toBeUpdated)
                {
                    MvccCoordinator.ValidateCanModifyRow(Engine, databaseName, _model.TableName, rowId, txContext.Snapshot, "UPDATE");
                }

                (List<Dictionary<string, object?>> newRows, List<RowKey> oldRowIds) = EvaluateAndVerifyConstraints(existingRows, databaseName);

                for (int i = 0; i < newRows.Count; i++)
                {
                    txContext.BufferUpdate(
                        _model.TableName,
                        oldRowIds[i],
                        newRows[i].ToDictionary(pair => pair.Key, pair => (object?)pair.Value, newRows[i].Comparer));
                }

                Messages.Add($"Rows affected: {newRows.Count}");
            }
            else
            {
                List<RowKey> toBeUpdated = IdentifyRowsToUpdate(databaseName);
                if (toBeUpdated.Count == 0)
                {
                    Messages.Add("Rows affected: 0");
                    return;
                }

                List<RowKey> rowWriteLocks = Locks.AcquireRowWriteLocks(databaseName, _model.TableName, toBeUpdated);
                int rowsAffected;

                try
                {
                    List<RowKey> revalidatedRowIds = RevalidateRowsAfterLock(databaseName, rowWriteLocks, toBeUpdated);
                    Dictionary<RowKey, Dictionary<string, object?>> revalidatedRows = Context.GetTableContents(revalidatedRowIds, _model.TableName, databaseName);

                    if (revalidatedRows.Count == 0)
                    {
                        rowsAffected = 0;
                    }
                    else
                    {
                        (List<Dictionary<string, object?>> newRows, List<RowKey> oldRowIds) = EvaluateAndVerifyConstraints(revalidatedRows, databaseName);

                        foreach (RowKey rowId in oldRowIds)
                        {
                            MvccCoordinator.ValidateCanModifyRow(Engine, databaseName, _model.TableName, rowId, null, "UPDATE");
                        }

                        ChangeRecorder? recorder = ChangeRecorder.TryCreate(Engine, databaseName);
                        ExecuteUpdate(newRows, oldRowIds, revalidatedRows, databaseName, statementTxId, recorder);
                        recorder?.Publish();
                        rowsAffected = newRows.Count;
                    }
                }
                finally
                {
                    Locks.ReleaseRowWriteLocks(databaseName, _model.TableName, rowWriteLocks);
                }

                Messages.Add($"Rows affected: {rowsAffected}");
            }
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            AddError(ex);
        }
    }

    /// <summary>
    /// Determines which row IDs need to be updated based on the WHERE expression.
    /// </summary>
    /// <param name="databaseName">The active database name.</param>
    /// <returns>A list of matching row IDs.</returns>
    private List<RowKey> IdentifyRowsToUpdate(string databaseName)
    {
        var whereStatement = new Statements.Where(_model.WhereExpression);
        return whereStatement.EvaluateWithoutJoin(_model.TableName, databaseName).ToList();
    }

    private List<RowKey> RevalidateRowsAfterLock(
        string databaseName,
        IReadOnlyList<RowKey> lockCoveredRowIds,
        IReadOnlyList<RowKey> originalCandidates)
    {
        if (originalCandidates.Count == 0)
        {
            return [];
        }

        if (IsTriviallyTrueWhere(_model.WhereExpression))
        {
            return [.. originalCandidates];
        }

        HashSet<RowKey> lockCovered = [.. lockCoveredRowIds];

        HashSet<RowKey> originalSet = [.. originalCandidates];

        return IdentifyRowsToUpdate(databaseName)
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

    private Dictionary<string, ExpressionNode> GetMaterializedSetExpressions(string databaseName)
    {
        if (_materializedSetExpressions != null)
        {
            return _materializedSetExpressions;
        }

        var tableService = new TableService(databaseName);
        tableService.AddTableDetail(new TableDetail(_model.TableName, null));

        _materializedSetExpressions = _model.SetExpressions.ToDictionary(
            pair => pair.Key,
            pair => SubqueryExpressionMaterializer.Materialize(pair.Value, databaseName, DataVoEngine.Current(), tableService));

        return _materializedSetExpressions;
    }

    /// <summary>
    /// Evaluates SET expressions to produce new rows and validates all integrity constraints (PKs, UKs, FKs).
    /// </summary>
    /// <param name="existingRows">Dictionary of existing row IDs mapping to old row data.</param>
    /// <param name="databaseName">The active database name.</param>
    /// <returns>A tuple containing the newly evaluated rows and the corresponding old row IDs.</returns>
    private (List<Dictionary<string, object?>> NewRows, List<RowKey> OldRowIds) EvaluateAndVerifyConstraints(
        Dictionary<RowKey, Dictionary<string, object?>> existingRows,
        string databaseName)
    {
        List<Dictionary<string, object?>> newRows = new(existingRows.Count);
        List<RowKey> oldRowIds = new(existingRows.Count);

        var primaryKeys = Catalog.GetTablePrimaryKeys(_model.TableName, databaseName);
        var uniqueKeys = Catalog.GetTableUniqueKeys(_model.TableName, databaseName);
        var foreignKeysList = Catalog.GetTableForeignKeys(_model.TableName, databaseName);
        var childFks = Catalog.GetChildForeignKeys(_model.TableName, databaseName);

        Dictionary<string, HashSet<string>> batchUniqueValues = InitializeBatchUniqueTracker(primaryKeys, uniqueKeys);
        List<Models.Catalog.Column> assignedColumns = [];
        foreach (Models.Catalog.Column column in Catalog.GetTableColumnsReadOnly(_model.TableName, databaseName))
        {
            if (_model.SetExpressions.ContainsKey(column.Name))
            {
                assignedColumns.Add(column);
            }
        }
        List<Models.Catalog.Column> lengthLimitedColumns = assignedColumns
            .FindAll(column => column.Length > 0 && column.Type.Equals("VARCHAR", StringComparison.OrdinalIgnoreCase));
        List<Models.Catalog.Column> notNullColumns = assignedColumns.FindAll(column => column.IsNotNull);

        int rowNumber = 1;
        foreach (var (rowId, oldRow) in existingRows)
        {
            oldRowIds.Add(rowId);

            // Create and add new row to the list of new rows
            var newRow = ApplySetExpressions(oldRow, databaseName);
            newRows.Add(newRow);

            foreach (Models.Catalog.Column column in notNullColumns)
            {
                if (!newRow.TryGetValue(column.Name, out object? assigned) || assigned is null || assigned is DBNull)
                {
                    throw ColumnNullRule.Violation(column.Name, rowNumber);
                }
            }

            foreach (Models.Catalog.Column column in lengthLimitedColumns)
            {
                if (newRow.TryGetValue(column.Name, out object? assigned)
                    && ColumnLengthRule.Exceeds(column, assigned, out int length))
                {
                    throw new ConstraintViolationException(ColumnLengthRule.Message(column.Name, column.Length, length, rowNumber))
                    {
                        ErrorCode = Contracts.Results.DataVoErrorCodes.ValueTooLong,
                    };
                }
            }

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
    /// <param name="databaseName">The active database name used for subquery materialization in SET expressions.</param>
    /// <returns>A new dictionary representing the modified row.</returns>
    private Dictionary<string, object?> ApplySetExpressions(Dictionary<string, object?> oldRow, string databaseName)
    {
        var newRow = new Dictionary<string, object?>(oldRow);

        Dictionary<string, ExpressionNode> setExpressions = GetMaterializedSetExpressions(databaseName);

        foreach (var setExpr in setExpressions)
        {
            string colName = setExpr.Key;
            object? newValue = ScalarEvaluator.Evaluate(setExpr.Value, oldRow);

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
        Dictionary<string, object?> newRow,
        Dictionary<string, object?> oldRow,
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
                    throw new EvaluationException($"Constraint violation: Primary key column {col} cannot be null in row {rowNumber}.");
                }

                if (val == null) continue; // Unique keys can be null

                string valStr = val.ToString()!;
                string oldValStr = oldRow.TryGetValue(col, out var oldVal) ? (oldVal?.ToString() ?? "null_val") : "null_val";

                // Fine if it hasn't actually mutated
                if (valStr == oldValStr) continue;

                if (!batchUniqueValues[col].Add(valStr))
                {
                    throw new EvaluationException($"Update conflict: Duplicate value '{valStr}' generated within the same batch for unique column {col}.");
                }

                string indexName = primaryKeys.Contains(col) ? $"_PK_{_model.TableName}" : $"_UK_{col}";
                bool duplicateExists = false;

                try
                {
                    duplicateExists = Indexes.IndexContainsKey(valStr, indexName, _model.TableName, databaseName);
                }
                catch
                {
                    // Fall back to row scan below.
                }

                if (!duplicateExists)
                {
                    var rows = Context.GetTableContents(_model.TableName, databaseName);
                    duplicateExists = rows
                        .Any(entry => entry.Value.TryGetValue(col, out var existing)
                                      && existing != null
                                      && string.Equals(existing.ToString(), valStr, StringComparison.Ordinal));
                }

                if (duplicateExists)
                {
                    throw new EvaluationException($"Constraint violation: Duplicate value '{valStr}' for unique column {col} in row {rowNumber}.");
                }
            }
        }
    }

    /// <summary>
    /// Validates that any updated foreign key column correctly references an existing parent Primary Key.
    /// </summary>
    private static void ValidateForeignKeyConstraints(
        Dictionary<string, object?> newRow,
        Dictionary<string, object?> oldRow,
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
                        throw new EvaluationException($"Foreign key violation: Value '{fkValStr}' does not reference an existing parent row for foreign key column {fk.AttributeName} in row {rowNumber}.");
                    }
                }
            }
        }
    }

    /// <summary>
    /// Validates that if a Primary Key is modified, it does not orphans records in child tables (RESTRICT enforcement).
    /// </summary>
    private void ValidateChildForeignKeyConstraints(
        Dictionary<string, object?> newRow,
        Dictionary<string, object?> oldRow,
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
                List<RowKey> childRowIds;

                try
                {
                    childRowIds = Indexes.FilterUsingIndex(oldParentValStr, childIndexName, childFk.ChildTable, databaseName).ToList();
                }
                catch
                {
                    childRowIds = FindChildRowsByTableScan(childFk.ChildTable, childFk.ChildColumn, oldParentValStr, databaseName);
                }

                childRowIds = childRowIds
                    .Where(id => id != 0 && Context.TableContainsRow(id, childFk.ChildTable, databaseName))
                    .ToList();

                if (childRowIds.Count > 0)
                {
                    throw new EvaluationException($"Foreign key violation: Cannot update {childFk.ParentColumn} ('{oldParentValStr}' -> '{newParentValStr}') in {_model.TableName} because {childRowIds.Count} row(s) in {childFk.ChildTable} depend on it.");
                }
            }
        }
    }

    /// <summary>
    /// Executes the update via the storage engine utilizing an out-of-place update architecture:
    /// Old records are deleted, and new records are inserted with identical RowIds or newly allocated ones.
    /// Index structures are purged and updated as well.
    /// </summary>
    private void ExecuteUpdate(
        List<Dictionary<string, object?>> newRows,
        List<RowKey> oldRowIds,
        Dictionary<RowKey, Dictionary<string, object?>> oldRowsById,
        string databaseName,
        long statementTxId,
        ChangeRecorder? recorder)
    {
        IReadOnlyList<IndexFile> indexFiles = Catalog.GetTableIndexesReadOnly(_model.TableName, databaseName);

        // Delete old records from storage & indexes
        Context.DeleteFromTable(oldRowIds, _model.TableName, databaseName);
        foreach (var index in indexFiles)
        {
            string indexName = index.IndexFileName ?? string.Empty;
            if (string.IsNullOrWhiteSpace(indexName))
            {
                continue;
            }

            string indexKind = index.IndexKind ?? string.Empty;

            if (Indexes.SupportsVectorIndexType(indexKind))
            {
                Indexes.DeleteFromVectorIndex(oldRowIds, indexName, _model.TableName, databaseName, indexKind);
            }
            else
            {
                // The old rows are known, so remove exact (old key, row id) pairs instead of scanning.
                var oldEntries = new List<(string Key, RowKey RowId)>(oldRowIds.Count);
                foreach (RowKey oldRowId in oldRowIds)
                {
                    // Same encoding as inserts; removing a pair that was never indexed is a no-op.
                    if (oldRowsById.TryGetValue(oldRowId, out Dictionary<string, object?>? oldRow)
                        && index.AttributeNames.All(oldRow.ContainsKey))
                    {
                        oldEntries.Add((IndexKeyEncoder.BuildKeyString(oldRow, index.AttributeNames), oldRowId));
                    }
                }

                Indexes.DeleteIndexEntries(oldEntries, indexName, _model.TableName, databaseName);
            }
        }

        // Insert new records into storage & indexes
        List<RowKey> assignedRowIds = Context.InsertIntoTable(newRows, _model.TableName, databaseName);
        for (int i = 0; i < newRows.Count; i++)
        {
            var newRow = newRows[i];
            RowKey oldRowId = oldRowIds[i];
            RowKey assignedRowId = assignedRowIds[i];
            MvccCoordinator.RegisterUpdateVersion(Engine, databaseName, _model.TableName, oldRowId, assignedRowId, statementTxId);

            if (recorder is { IsEnabled: true } && oldRowsById.TryGetValue(oldRowId, out var oldRow))
            {
                recorder.RecordUpdate(_model.TableName, assignedRowId, oldRow, newRow);
            }
        }

        InsertUpdatedIndexesBatch(newRows, assignedRowIds, indexFiles, databaseName);

        if (ImplicitWalCommit.IsEnabled(Engine))
        {
            var operations = new List<WalOperation>(newRows.Count);
            for (int i = 0; i < newRows.Count; i++)
            {
                operations.Add(new WalOperation
                {
                    OperationType = WalOperationType.Update,
                    TableName = _model.TableName,
                    RowId = RowKeyConversions.ToRowId(oldRowIds[i]),
                    UpdatedColumns = new Dictionary<string, object?>(newRows[i], newRows[i].Comparer),
                });
            }

            ImplicitWalCommit.CommitIfEnabled(Engine, databaseName, statementTxId, operations);
        }
    }

    private void InsertUpdatedIndexesBatch(
        IReadOnlyList<Dictionary<string, object?>> newRows,
        IReadOnlyList<RowKey> assignedRowIds,
        IReadOnlyList<IndexFile> indexFiles,
        string databaseName)
    {
        foreach (var index in indexFiles)
        {
            string indexName = index.IndexFileName ?? string.Empty;
            if (string.IsNullOrWhiteSpace(indexName))
            {
                continue;
            }

            string indexKind = index.IndexKind ?? string.Empty;
            if (Indexes.SupportsVectorIndexType(indexKind))
            {
                InsertUpdatedVectorIndex(newRows, assignedRowIds, index, indexName, indexKind, databaseName);
                continue;
            }

            List<(string Value, RowKey RowId)> entries = BuildScalarIndexEntries(newRows, assignedRowIds, index.AttributeNames);

            if (IsPrimaryKeyIndex(indexName)
                && TryBuildIntegerIndexEntries(newRows, assignedRowIds, index.AttributeNames, out List<(long Key, RowKey RowId)>? integerPrimaryEntries)
                && integerPrimaryEntries is not null)
            {
                Indexes.InsertIntegerPrimaryKeys(integerPrimaryEntries, indexName, _model.TableName, databaseName);
            }
            else if (IsPrimaryKeyIndex(indexName)
                && TryBuildGuidIndexEntries(newRows, assignedRowIds, index.AttributeNames, out List<(Guid Key, RowKey RowId)>? guidPrimaryEntries)
                && guidPrimaryEntries is not null)
            {
                Indexes.InsertGuidPrimaryKeys(guidPrimaryEntries, indexName, _model.TableName, databaseName);
            }
            else if (TryBuildIntegerIndexEntries(newRows, assignedRowIds, index.AttributeNames, out List<(long Key, RowKey RowId)>? integerIndexEntries)
                && integerIndexEntries is not null)
            {
                Indexes.InsertIntegerIndexEntries(integerIndexEntries, indexName, _model.TableName, databaseName);
            }
            else if (TryBuildGuidIndexEntries(newRows, assignedRowIds, index.AttributeNames, out List<(Guid Key, RowKey RowId)>? guidIndexEntries)
                && guidIndexEntries is not null)
            {
                Indexes.InsertGuidIndexEntries(guidIndexEntries, indexName, _model.TableName, databaseName);
            }

            // Fast lanes mirror hot equality lookups. The scalar BTree remains the authoritative index
            // representation for dirty tracking, persistence, and snapshot rebuild semantics.
            Indexes.InsertManyIntoIndex(entries, indexName, _model.TableName, databaseName);
        }
    }

    private static List<(string Value, RowKey RowId)> BuildScalarIndexEntries(
        IReadOnlyList<Dictionary<string, object?>> newRows,
        IReadOnlyList<RowKey> assignedRowIds,
        IReadOnlyList<string> attributeNames)
    {
        var entries = new List<(string Value, RowKey RowId)>(newRows.Count);
        for (int i = 0; i < newRows.Count; i++)
        {
            Dictionary<string, object?> newRow = newRows[i];
            if (attributeNames.Any(attr => !newRow.TryGetValue(attr, out var attrValue) || attrValue == null))
            {
                continue;
            }

            string indexValue = IndexKeyEncoder.BuildKeyString(newRow, attributeNames);
            entries.Add((indexValue, assignedRowIds[i]));
        }

        return entries;
    }

    private void InsertUpdatedVectorIndex(
        IReadOnlyList<Dictionary<string, object?>> newRows,
        IReadOnlyList<RowKey> assignedRowIds,
        IndexFile index,
        string indexName,
        string indexKind,
        string databaseName)
    {
        if (index.AttributeNames.Count != 1)
        {
            throw new BindingException($"Vector index '{indexName}' (type '{indexKind}') must reference exactly one VECTOR column.");
        }

        string vectorColumn = index.AttributeNames[0];
        for (int i = 0; i < newRows.Count; i++)
        {
            Dictionary<string, object?> newRow = newRows[i];
            if (!newRow.TryGetValue(vectorColumn, out object? value) || value is null)
            {
                continue;
            }

            if (!VectorParser.TryCoerceToVector(value, out float[] vector))
            {
                throw new EvaluationException($"Cannot coerce value of '{vectorColumn}' into VECTOR for index '{indexName}'.");
            }

            Indexes.InsertIntoVectorIndex(vector, assignedRowIds[i], indexName, _model.TableName, databaseName, indexKind);
        }
    }

    private bool IsPrimaryKeyIndex(string indexName) =>
        string.Equals(indexName, $"_PK_{_model.TableName}", StringComparison.OrdinalIgnoreCase);

    private static bool TryBuildIntegerIndexEntries(
        IReadOnlyList<Dictionary<string, object?>> rows,
        IReadOnlyList<RowKey> rowIds,
        IReadOnlyList<string> attributeNames,
        out List<(long Key, RowKey RowId)>? entries)
    {
        entries = null;
        if (attributeNames.Count != 1)
        {
            return false;
        }

        string column = attributeNames[0];
        var built = new List<(long Key, RowKey RowId)>(rows.Count);
        for (int i = 0; i < rows.Count; i++)
        {
            if (!rows[i].TryGetValue(column, out object? value) || value is null)
            {
                continue;
            }

            if (!TryCoerceInt64(value, out long key))
            {
                return false;
            }

            built.Add((key, rowIds[i]));
        }

        entries = built;
        return true;
    }

    private static bool TryBuildGuidIndexEntries(
        IReadOnlyList<Dictionary<string, object?>> rows,
        IReadOnlyList<RowKey> rowIds,
        IReadOnlyList<string> attributeNames,
        out List<(Guid Key, RowKey RowId)>? entries)
    {
        entries = null;
        if (attributeNames.Count != 1)
        {
            return false;
        }

        string column = attributeNames[0];
        var built = new List<(Guid Key, RowKey RowId)>(rows.Count);
        for (int i = 0; i < rows.Count; i++)
        {
            if (!rows[i].TryGetValue(column, out object? value) || value is null)
            {
                continue;
            }

            if (!TryCoerceGuid(value, out Guid key))
            {
                return false;
            }

            built.Add((key, rowIds[i]));
        }

        entries = built;
        return true;
    }

    private static bool TryCoerceInt64(object value, out long key)
    {
        switch (value)
        {
            case int intValue:
                key = intValue;
                return true;
            case long longValue:
                key = longValue;
                return true;
            default:
                return long.TryParse(value.ToString(), out key);
        }
    }

    private static bool TryCoerceGuid(object value, out Guid key)
    {
        if (value is Guid guid)
        {
            key = guid;
            return true;
        }

        return Guid.TryParse(value.ToString(), out key);
    }

    /// <summary>
    /// Queries the IndexManager to determine if a required foreign key value exists in the parent's Primary Key index.
    /// </summary>
    /// <returns>True if the reference exists; otherwise false.</returns>
    private static bool CheckForeignKeyConstraint(ForeignKey foreignKey, string columnValue, string databaseName)
    {
        foreach (var reference in foreignKey.References)
        {
            if (ReferenceExists(reference.ReferenceTableName, reference.ReferenceAttributeName, columnValue, databaseName))
            {
                continue;
            }

            return false;
        }

        return true;
    }

    private static bool ReferenceExists(string tableName, string columnName, string expectedValue, string databaseName)
    {
        try
        {
            if (DataVoEngine.Current().IndexManager.IndexContainsKey(expectedValue, $"_PK_{tableName}", tableName, databaseName))
            {
                return true;
            }
        }
        catch
        {
            // Fall back to a table scan below.
        }

        var rows = DataVoEngine.Current().StorageContext.GetTableContents(tableName, databaseName);
        foreach (var row in rows.Values)
        {
            if (row.TryGetValue(columnName, out var value) && value != null && string.Equals(value.ToString(), expectedValue, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Falls back to a linear scan to find dependent child rows if the child table lacks an index on the foreign key column.
    /// </summary>
    /// <returns>A list of Row IDs representing matched child records.</returns>
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
