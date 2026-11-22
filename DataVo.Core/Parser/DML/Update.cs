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

            var txContext = Transactions.GetContext(session);
            long statementTxId = MvccCoordinator.ResolveStatementTransactionId(Engine, txContext?.TransactionId);
            if (txContext != null)
            {
                Messages.Add($"Rows affected: {UpdateInTransaction(txContext, databaseName)}");
                return;
            }

            _ = GetMaterializedSetExpressions(databaseName);
            AcquiredDmlRowScope lockScope = DmlRowLockCoordinator.Acquire(
                Locks,
                databaseName,
                _model.TableName,
                _model.WhereExpression,
                () => IdentifyRowsToUpdate(databaseName));

            if (lockScope.TargetRowIds.Count == 0)
            {
                Messages.Add("Rows affected: 0");
                return;
            }

            int rowsAffected;

            try
            {
                Dictionary<RowKey, Dictionary<string, object?>> revalidatedRows = Context.GetTableContents(lockScope.TargetRowIds, _model.TableName, databaseName);

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
                    ExecuteUpdate(newRows, oldRowIds, revalidatedRows, databaseName, statementTxId, recorder, lockScope);
                    recorder?.Publish();
                    rowsAffected = newRows.Count;
                }
            }
            finally
            {
                DmlRowLockCoordinator.Release(Locks, databaseName, _model.TableName, lockScope);
            }

            Messages.Add($"Rows affected: {rowsAffected}");
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            AddError(ex);
        }
    }

    /// <summary>
    /// Buffers the update inside an explicit transaction. The WHERE clause, the SET expressions (and their
    /// subqueries) and the constraint checks all read through the transaction's snapshot and its own uncommitted
    /// writes, so rows it inserted or updated earlier match by their current values. A row the transaction inserted
    /// is updated in its insert buffer; a stored row gets one coalesced buffered update.
    /// </summary>
    /// <returns>The number of rows updated.</returns>
    private int UpdateInTransaction(TransactionContext txContext, string databaseName)
    {
        using IDisposable scope = MvccExecutionScope.PushTransaction(txContext);
        _ = GetMaterializedSetExpressions(databaseName);

        List<RowKey> toBeUpdated = IdentifyRowsToUpdate(databaseName);
        txContext.ExcludeDeleted(_model.TableName, toBeUpdated);
        if (toBeUpdated.Count == 0)
        {
            return 0;
        }

        Dictionary<RowKey, Dictionary<string, object?>> existingRows = Context.GetTableContents(toBeUpdated, _model.TableName, databaseName);
        foreach (RowKey rowId in existingRows.Keys)
        {
            if (!TransactionContext.IsBufferedRowKey(rowId))
            {
                MvccCoordinator.ValidateCanModifyRow(Engine, databaseName, _model.TableName, rowId, txContext.Snapshot, "UPDATE");
            }
        }

        bool modifiesKeyColumns = ModifiesKeyColumns(databaseName);
        InsertRowService.PendingKeys? pending = modifiesKeyColumns
            ? new InsertRowService(Engine, Context, Catalog, Indexes)
                .BuildPendingKeys(txContext, databaseName, _model.TableName, includeInserts: true)
            : null;
        (List<Dictionary<string, object?>> newRows, List<RowKey> oldRowIds) = EvaluateAndVerifyConstraints(existingRows, databaseName, pending);

        for (int i = 0; i < newRows.Count; i++)
        {
            var newRow = new Dictionary<string, object?>(newRows[i], StringComparer.OrdinalIgnoreCase);
            if (TransactionContext.IsBufferedRowKey(oldRowIds[i]))
            {
                txContext.ReplaceBufferedInsert(_model.TableName, oldRowIds[i], newRow, keyColumnsUnchanged: !modifiesKeyColumns);
            }
            else
            {
                txContext.BufferUpdate(_model.TableName, oldRowIds[i], newRow, keyColumnsUnchanged: !modifiesKeyColumns);
            }
        }

        return newRows.Count;
    }

    private bool ModifiesKeyColumns(string databaseName)
    {
        foreach (string keyColumn in Catalog.GetTablePrimaryKeys(_model.TableName, databaseName))
        {
            if (_model.SetExpressions.ContainsKey(keyColumn))
            {
                return true;
            }
        }

        foreach (string uniqueColumn in Catalog.GetTableUniqueKeys(_model.TableName, databaseName))
        {
            if (_model.SetExpressions.ContainsKey(uniqueColumn))
            {
                return true;
            }
        }

        return false;
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
    /// <param name="pending">Inside a transaction, the keys its buffered writes free and take.</param>
    /// <returns>A tuple containing the newly evaluated rows and the corresponding old row IDs.</returns>
    private (List<Dictionary<string, object?>> NewRows, List<RowKey> OldRowIds) EvaluateAndVerifyConstraints(
        Dictionary<RowKey, Dictionary<string, object?>> existingRows,
        string databaseName,
        InsertRowService.PendingKeys? pending = null)
    {
        List<Dictionary<string, object?>> newRows = new(existingRows.Count);
        List<RowKey> oldRowIds = new(existingRows.Count);

        var primaryKeys = Catalog.GetTablePrimaryKeys(_model.TableName, databaseName);
        var uniqueKeys = Catalog.GetTableUniqueKeys(_model.TableName, databaseName);
        var foreignKeysList = Catalog.GetTableForeignKeys(_model.TableName, databaseName);
        var childFks = Catalog.GetChildForeignKeys(_model.TableName, databaseName);

        var keyValidator = new UpdateKeyValidator(Indexes, Context, _model.TableName, databaseName, pending);
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
            keyValidator.Validate(newRow, oldRow, primaryKeys, uniqueKeys, rowNumber);
            ValidateForeignKeyConstraints(newRow, oldRow, foreignKeysList, rowNumber, databaseName);
            ValidateChildForeignKeyConstraints(newRow, oldRow, childFks, databaseName);

            rowNumber++;
        }

        return (newRows, oldRowIds);
    }


    /// <summary>
    /// Applies the SQL SET expressions to the old row to calculate its new modified state.
    /// </summary>
    /// <param name="oldRow">The original row record.</param>
    /// <param name="databaseName">The active database name used for subquery materialization in SET expressions.</param>
    /// <returns>A new dictionary representing the modified row.</returns>
    private Dictionary<string, object?> ApplySetExpressions(Dictionary<string, object?> oldRow, string databaseName)
    {
        var newRow = new Dictionary<string, object?>(oldRow, StringComparer.OrdinalIgnoreCase);

        Dictionary<string, ExpressionNode> setExpressions = GetMaterializedSetExpressions(databaseName);

        foreach (var setExpr in setExpressions)
        {
            string colName = setExpr.Key;
            object? newValue = ScalarEvaluator.Evaluate(setExpr.Value, oldRow);

            if (setExpr.Value is not BoundValueLiteralNode && newValue is string s && s.StartsWith("'") && s.EndsWith("'"))
            {
                newValue = Utils.SqlLiteralText.Unquote(s);
            }

            newRow[colName] = newValue!;
        }

        return newRow;
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
        ChangeRecorder? recorder,
        AcquiredDmlRowScope lockScope)
    {
        IReadOnlyList<IndexFile> indexFiles = Catalog.GetTableIndexesReadOnly(_model.TableName, databaseName);

        if (recorder is { WantsAnnouncements: true })
        {
            for (int i = 0; i < oldRowIds.Count; i++)
            {
                if (oldRowsById.TryGetValue(oldRowIds[i], out var oldRow))
                {
                    recorder.AnnounceUpdate(_model.TableName, oldRowIds[i], oldRow, newRows[i]);
                }
            }
        }

        var oldRows = new List<Dictionary<string, object?>>(oldRowIds.Count);
        for (int i = 0; i < oldRowIds.Count; i++)
        {
            oldRows.Add(oldRowsById[oldRowIds[i]]);
        }

        bool isClustered = Context.ResolveClusteredKey(databaseName, _model.TableName) is not null;
        if (isClustered)
        {
            Context.DeleteFromTable(oldRowIds, _model.TableName, databaseName);
        }

        List<RowKey> assignedRowIds = Context.InsertIntoTable(newRows, _model.TableName, databaseName);
        List<RowKey> relocatedRowLocks = DmlRowLockCoordinator.AcquireRelocatedRowWriteLocks(
            Locks,
            databaseName,
            _model.TableName,
            assignedRowIds,
            lockScope.RowWriteLocks,
            skipLocking: isClustered || lockScope.HoldsTableWriteLock);

        try
        {
            for (int i = 0; i < newRows.Count; i++)
            {
                var newRow = newRows[i];
                RowKey oldRowId = oldRowIds[i];
                RowKey assignedRowId = assignedRowIds[i];
                Dictionary<string, object?> oldRow = oldRows[i];
                MvccCoordinator.RegisterUpdateVersion(Engine, databaseName, _model.TableName, oldRowId, assignedRowId, statementTxId, oldRow);

                if (recorder is { IsEnabled: true })
                {
                    recorder.RecordUpdate(_model.TableName, assignedRowId, oldRow, newRow);
                }
            }

            foreach (var index in indexFiles)
            {
                CompiledQueries.DataVoCompiledQuery.MaintainIndexForUpdate(
                    Engine,
                    _model.TableName,
                    databaseName,
                    index,
                    oldRowIds,
                    oldRows,
                    assignedRowIds,
                    newRows);
            }

            if (!isClustered)
            {
                Context.DeleteFromTable(oldRowIds, _model.TableName, databaseName);
            }

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
        finally
        {
            if (relocatedRowLocks.Count > 0)
            {
                Locks.ReleaseRowWriteLocks(databaseName, _model.TableName, relocatedRowLocks);
            }
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
