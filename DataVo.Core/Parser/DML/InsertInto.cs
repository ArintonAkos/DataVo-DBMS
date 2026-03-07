using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Models.DML;
using DataVo.Core.Parser.Actions;
using DataVo.Core.BTree;
using DataVo.Core.Cache;
using DataVo.Core.StorageEngine;
using System.Text.RegularExpressions;
using DataVo.Core.Parser.AST;

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
            string databaseName = CacheStorage.Get(session)
                ?? throw new Exception("No database in use!");

            int rowsAffected = ProcessAndInsertTableRows(databaseName);

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
    /// <returns>The total number of rows securely pushed to the database.</returns>
    private int ProcessAndInsertTableRows(string databaseName)
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
                    MakeInsertion(rowDict, indexFiles, databaseName);
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
        return IndexManager.Instance.IndexContainsKey(
            tableColumn.Value!, 
            $"_UK_{tableColumn.Name}", 
            _model.TableName, 
            databaseName);
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
        if (IndexManager.Instance.IndexContainsKey(id, $"_PK_{_model.TableName}", _model.TableName, databaseName))
        {
            LogInsertError($"Primary key violation in row {rowNumber}!");
            return false;
        }

        return true;
    }

    private void LogInsertError(string message)
    {
        Messages.Add(message);
        Logger.Error(message);
    }

    /// <summary>
    /// Dispatches the final, validated row structure to the physical storage engine and linked B-Tree indexes.
    /// </summary>
    private void MakeInsertion(Dictionary<string, dynamic> rowDict, List<IndexFile> indexFiles, string databaseName)
    {
        long assignedRowId = StorageContext.Instance.InsertOneIntoTable(rowDict, _model.TableName, databaseName);

        foreach (var index in indexFiles)
        {
            if (index.AttributeNames.Any(attr => rowDict[attr] == null)) continue;
            
            string indexValue = IndexKeyEncoder.BuildKeyString(rowDict, index.AttributeNames);

            IndexManager.Instance.InsertIntoIndex(indexValue, assignedRowId, index.IndexFileName, _model.TableName, databaseName);
        }
    }

    /// <summary>
    /// Dynamically scans parent indexing nodes to confirm the validity of requested foreign key relationships constraints restrictively.
    /// </summary>
    private bool CheckForeignKeyConstraint(ForeignKey foreignKey, string columnValue, string databaseName)
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
}
