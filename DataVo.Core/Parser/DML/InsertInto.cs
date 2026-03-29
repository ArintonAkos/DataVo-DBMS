using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Models.DML;
using DataVo.Core.Parser.Actions;
using DataVo.Core.BTree;
using DataVo.Core.StorageEngine;
using System.Text.RegularExpressions;
using DataVo.Core.Parser.AST;
using DataVo.Core.Transactions;
using DataVo.Core.Utils;
using DataVo.Core.MVCC;

namespace DataVo.Core.Parser.DML;

/// <summary>
/// Executes the SQL INSERT INTO command to append new records into a table.
/// Handles static literal inputs mapping against the active database schema constraints.
/// </summary>
/// <example>
/// <code>
/// // Example SQL: INSERT INTO Users (Id, Status) VALUES (1, 'Active');
/// var insertAction = new InsertInto(astNode);
/// insertAction.PerformAction(sessionId);
/// </code>
/// </example>
internal class InsertInto(InsertIntoStatement ast) : BaseDbAction
{
    private readonly InsertIntoModel _model = InsertIntoModel.FromAst(ast);

    /// <summary>
    /// Executes the logical insertion operation sequentially on behalf of the user transaction.
    /// Retrieves active session bounds and dispatches logical operations to parsing systems.
    /// </summary>
    /// <param name="session">The unique identifier of the user session executing the action.</param>
    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = GetDatabaseName(session);

            var txContext = Transactions.GetContext(session);
            long statementTxId = MvccCoordinator.ResolveStatementTransactionId(Engine, txContext?.TransactionId);
            int rowsAffected;

            if (txContext != null)
            {
                rowsAffected = ProcessAndInsertTableRows(databaseName, txContext, statementTxId);
            }
            else
            {
                Locks.AcquireWriteLock(databaseName, _model.TableName);

                try
                {
                    rowsAffected = ProcessAndInsertTableRows(databaseName, null, statementTxId);
                }
                finally
                {
                    Locks.ReleaseWriteLock(databaseName, _model.TableName);
                }
            }

            Messages.Add($"Rows affected: {rowsAffected}");
        }
        catch (Exception e)
        {
            Messages.Add(e.Message);
            Logger.Error(e.ToString());
        }
    }

    /// <summary>
    /// Parses the AST mappings and converts raw strings efficiently into B-Tree mapped representations.
    /// Verifies SQL constraints (UNIQUE, PRIMARY, FOREIGN KEY) dynamically during mapping.
    /// </summary>
    /// <param name="databaseName">The current active context database name.</param>
    /// <param name="txContext">The active transaction context, or <c>null</c> for auto-commit mode.</param>
    /// <param name="statementTxId">The MVCC transaction identifier for this statement.</param>
    /// <returns>The total number of rows securely pushed to the database.</returns>
    private int ProcessAndInsertTableRows(string databaseName, TransactionContext? txContext, long statementTxId)
    {
        List<string> primaryKeys = Catalog.GetTablePrimaryKeys(_model.TableName, databaseName);
        List<string> uniqueKeys = Catalog.GetTableUniqueKeys(_model.TableName, databaseName);
        List<ForeignKey> foreignKeys = Catalog.GetTableForeignKeys(_model.TableName, databaseName);
        List<IndexFile> indexFiles = Catalog.GetTableIndexes(_model.TableName, databaseName);
        List<Column> tableColumns = Catalog.GetTableColumns(_model.TableName, databaseName);

        VerifyTableColumnsExist(tableColumns);

        var primaryKeySet = primaryKeys.ToHashSet();
        var uniqueKeySet = uniqueKeys.ToHashSet();
        var foreignKeysByAttribute = foreignKeys
            .GroupBy(foreignKey => foreignKey.AttributeName)
            .ToDictionary(group => group.Key, group => group.First());

        int rowNumber = 0;
        int rowsAffected = 0;

        bool hasColumns = _model.Columns.Count > 0;
        var insertColumnToIndex = hasColumns
            ? _model.Columns
                .Select((columnName, index) => new { columnName, index })
                .ToDictionary(entry => entry.columnName, entry => entry.index)
            : null;

        foreach (var rawRow in _model.RawRows)
        {
            rowNumber++;
            VerifyRowColumnCountMatches(rawRow, tableColumns.Count, hasColumns);

            if (TryParseRow(
                rawRow,
                tableColumns,
                hasColumns,
                insertColumnToIndex,
                uniqueKeySet,
                foreignKeysByAttribute,
                rowNumber,
                databaseName,
                out var rowDict))
            {
                if (VerifyPrimaryKeys(rowDict, primaryKeys, rowNumber, databaseName))
                {
                    if (txContext != null)
                    {
                        txContext.BufferInsert(
                            _model.TableName,
                            rowDict.ToDictionary(pair => pair.Key, pair => (object?)pair.Value, rowDict.Comparer));
                    }
                    else
                    {
                        MakeInsertion(rowDict, indexFiles, databaseName, statementTxId);
                    }
                    rowsAffected++;
                }
            }
        }

        return rowsAffected;
    }

    /// <summary>
    /// Confirms that user-defined explicit columns within the INSERT actually exist inside the table's schema.
    /// </summary>
    /// <param name="tableColumns">The columns sourced directly from the database schema context.</param>
    private void VerifyTableColumnsExist(List<Column> tableColumns)
    {
        var tableColumnNameSet = tableColumns.Select(column => column.Name).ToHashSet();
        foreach (var columnName in _model.Columns)
        {
            if (!tableColumnNameSet.Contains(columnName))
            {
                throw new Exception($"Column {columnName} doesn't exist in table {_model.TableName}!");
            }
        }
    }

    /// <summary>
    /// Safely ensures the parameter value sizes precisely mirror the column parameter targets dynamically.
    /// </summary>
    private void VerifyRowColumnCountMatches(List<string> rawRow, int tableColumnCount, bool hasColumns)
    {
        if (!hasColumns && rawRow.Count != tableColumnCount)
        {
            throw new Exception($"The number of values provided in a row must be the same as " +
                                $"the number of columns in the table when columns are not specified. (RawRow: {rawRow.Count}, TableColumns: {tableColumnCount})");
        }

        if (hasColumns && rawRow.Count != _model.Columns.Count)
        {
            throw new Exception("The number of values provided in a row must be the same as " +
                                "the number of columns provided inside the parenthesis after the table name attribute.");
        }
    }

    /// <summary>
    /// Maps AST extracted string values cleanly into strongly typed column representations sequentially.
    /// Injects configured DEFAULT values for unprovided column instances.
    /// </summary>
    /// <returns>True if the row is correctly typed and valid; otherwise false signifying rejection.</returns>
    private bool TryParseRow(
        List<string> rawRow,
        List<Column> tableColumns,
        bool hasColumns,
        Dictionary<string, int>? insertColumnToIndex,
        HashSet<string> uniqueKeySet,
        Dictionary<string, ForeignKey> foreignKeysByAttribute,
        int rowNumber,
        string databaseName,
        out Dictionary<string, dynamic> rowDict)
    {
        rowDict = new Dictionary<string, dynamic>();

        for (int i = 0; i < tableColumns.Count; i++)
        {
            Column tableColumn = tableColumns[i];

            string rawValue = ResolveRawValue(tableColumn, rawRow, hasColumns, insertColumnToIndex, i);

            tableColumn.Value = rawValue.Replace("'", "");

            // Prioritize mapped AST Value or explicitly maintain user-injected nulls
            dynamic? parsedValue = tableColumn.ParsedValue;

            if (parsedValue == null && rawValue.ToLowerInvariant() != "null")
            {
                LogInsertError($"Type of argument doesn't match with column type in row {rowNumber}!");
                return false;
            }

            rowDict[tableColumn.Name] = parsedValue!;

            if (parsedValue != null && uniqueKeySet.Contains(tableColumn.Name) && VerifyUniqueConstraint(tableColumn, databaseName))
            {
                LogInsertError($"Unique key violation in row {rowNumber}!");
                return false;
            }

            if (foreignKeysByAttribute.TryGetValue(tableColumn.Name, out ForeignKey? foreignKey) &&
                !CheckForeignKeyConstraint(foreignKey, tableColumn.Value, databaseName))
            {
                LogInsertError($"Foreign key violation in row {rowNumber}!");
                return false;
            }
        }

        return true;
    }

    private string ResolveRawValue(
        Column tableColumn,
        List<string> rawRow,
        bool hasColumns,
        Dictionary<string, int>? insertColumnToIndex,
        int index)
    {
        if (!hasColumns) return rawRow[index];

        if (insertColumnToIndex!.TryGetValue(tableColumn.Name, out int colIndex))
        {
            return rawRow[colIndex];
        }

        return tableColumn.DefaultValue ?? "null";
    }

    private bool VerifyUniqueConstraint(Column tableColumn, string databaseName)
    {
        string candidate = tableColumn.Value!;

        try
        {
            if (Indexes.IndexContainsKey(candidate, $"_UK_{tableColumn.Name}", _model.TableName, databaseName))
            {
                return true;
            }
        }
        catch
        {
            // Fall back to a row scan when the index is unavailable.
        }

        var rows = Context.GetTableContents(_model.TableName, databaseName);
        return rows.Values.Any(row => row.TryGetValue(tableColumn.Name, out var value)
                                      && value != null
                                      && string.Equals(value.ToString(), candidate, StringComparison.Ordinal));
    }

    /// <summary>
    /// Verifies the completed composite ID string does not collide with preexisting records to preserve Primary Keys.
    /// </summary>
    private bool VerifyPrimaryKeys(
        Dictionary<string, dynamic> rowDict,
        List<string> primaryKeys,
        int rowNumber,
        string databaseName)
    {
        if (primaryKeys.Count == 0) return true;

        if (primaryKeys.Any(pk => rowDict[pk] == null))
        {
            LogInsertError($"Primary key cannot be null in row {rowNumber}!");
            return false;
        }

        string id = IndexKeyEncoder.BuildKeyString(rowDict, primaryKeys);
        bool exists = false;

        try
        {
            exists = Indexes.IndexContainsKey(id, $"_PK_{_model.TableName}", _model.TableName, databaseName);
        }
        catch
        {
            // Fall back to row scan below.
        }

        if (!exists)
        {
            var rows = Context.GetTableContents(_model.TableName, databaseName);
            exists = rows.Values.Any(row => string.Equals(IndexKeyEncoder.BuildKeyString(row, primaryKeys), id, StringComparison.Ordinal));
        }

        if (exists)
        {
            LogInsertError($"Primary key violation in row {rowNumber}!");
            return false;
        }

        return true;
    }

    private void LogInsertError(string message)
    {
        Messages.Add(message);

        // Constraint rejections are expected outcomes in contention/fuzz scenarios
        // and should not flood logs as hard errors.
        if (message.Contains("Primary key violation", StringComparison.OrdinalIgnoreCase)
            || message.Contains("Unique key violation", StringComparison.OrdinalIgnoreCase)
            || message.Contains("Foreign key violation", StringComparison.OrdinalIgnoreCase))
        {
            // Expected business-rule rejection paths can be very frequent in contention tests.
            // Keep them in action messages but avoid console spam.
            return;
        }

        Logger.Error(message);
    }

    /// <summary>
    /// Dispatches the final, validated row structure to the physical storage engine and linked B-Tree indexes.
    /// </summary>
    private void MakeInsertion(Dictionary<string, dynamic> rowDict, List<IndexFile> indexFiles, string databaseName, long statementTxId)
    {
        long assignedRowId = Context.InsertOneIntoTable(rowDict, _model.TableName, databaseName);
        MvccCoordinator.RegisterInsertVersion(Engine, databaseName, _model.TableName, assignedRowId, statementTxId);

        foreach (var index in indexFiles)
        {
            if (index.AttributeNames.Any(attr => rowDict[attr] == null)) continue;

            string indexName = index.IndexFileName ?? string.Empty;
            if (string.IsNullOrWhiteSpace(indexName))
            {
                continue;
            }

            string indexKind = index.IndexKind ?? string.Empty;

            if (Indexes.SupportsVectorIndexType(indexKind))
            {
                if (index.AttributeNames.Count != 1)
                {
                    throw new Exception($"Vector index '{indexName}' (type '{indexKind}') must reference exactly one VECTOR column.");
                }

                string vectorColumn = index.AttributeNames[0];
                if (!VectorParser.TryCoerceToVector(rowDict[vectorColumn], out float[] vector))
                {
                    throw new Exception($"Cannot coerce value of '{vectorColumn}' into VECTOR for index '{indexName}'.");
                }

                Indexes.InsertIntoVectorIndex(vector, assignedRowId, indexName, _model.TableName, databaseName, indexKind);
                continue;
            }

            string indexValue = IndexKeyEncoder.BuildKeyString(rowDict, index.AttributeNames);
            Indexes.InsertIntoIndex(indexValue, assignedRowId, indexName, _model.TableName, databaseName);
        }
    }

    /// <summary>
    /// Dynamically scans parent indexing nodes to confirm the validity of requested foreign key relationships constraints restrictively.
    /// </summary>
    private bool CheckForeignKeyConstraint(ForeignKey foreignKey, string columnValue, string databaseName)
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

    private bool ReferenceExists(string tableName, string columnName, string expectedValue, string databaseName)
    {
        try
        {
            if (Indexes.IndexContainsKey(expectedValue, $"_PK_{tableName}", tableName, databaseName))
            {
                return true;
            }
        }
        catch
        {
            // Fall back to a table scan below.
        }

        var rows = Context.GetTableContents(tableName, databaseName);
        foreach (var row in rows.Values)
        {
            if (row.TryGetValue(columnName, out var value) && value != null && string.Equals(value.ToString(), expectedValue, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}
