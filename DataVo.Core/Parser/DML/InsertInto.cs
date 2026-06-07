using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Models.DML;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Exceptions;
using DataVo.Core.Parser.AST;
using DataVo.Core.Transactions;
using DataVo.Core.MVCC;
using DataVo.Core.Runtime.Changes;

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
                rowsAffected = ProcessAndInsertTableRows(databaseName, txContext, statementTxId, recorder: null);
            }
            else
            {
                ChangeRecorder? recorder = ChangeRecorder.TryCreate(Engine, databaseName);
                Locks.AcquireWriteLock(databaseName, _model.TableName);

                try
                {
                    rowsAffected = ProcessAndInsertTableRows(databaseName, null, statementTxId, recorder);
                }
                finally
                {
                    Locks.ReleaseWriteLock(databaseName, _model.TableName);
                }

                recorder?.Publish();
            }

            Messages.Add($"Rows affected: {rowsAffected}");
        }
        catch (Exception e)
        {
            AddError(e);
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
    /// <param name="recorder">The change recorder that captures inserted row images for reactive notifications, or <c>null</c> when change capture is disabled.</param>
    /// <returns>The total number of rows securely pushed to the database.</returns>
    private int ProcessAndInsertTableRows(string databaseName, TransactionContext? txContext, long statementTxId, ChangeRecorder? recorder)
    {
        var tableColumns = Catalog.GetTableColumns(_model.TableName, databaseName);
        VerifyTableColumnsExist(tableColumns);

        bool hasColumns = _model.Columns.Count > 0;
        var inputRows = new List<IReadOnlyDictionary<string, object?>>(_model.RawRows.Count);

        foreach (List<string> rawRow in _model.RawRows)
        {
            VerifyRowColumnCountMatches(rawRow, tableColumns.Count, hasColumns);
            inputRows.Add(BuildInputRow(rawRow, tableColumns, hasColumns));
        }

        var service = new InsertRowService(Engine, Context, Catalog, Indexes);
        InsertRowsResult result = service.InsertRows(databaseName, _model.TableName, inputRows, txContext, statementTxId, recorder);

        foreach (string message in result.Messages)
        {
            Messages.Add(message);
        }

        return result.AcceptedRowCount;
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
                throw new BindingException($"Column {columnName} doesn't exist in table {_model.TableName}!");
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
            throw new BindingException($"The number of values provided in a row must be the same as " +
                                $"the number of columns in the table when columns are not specified. (RawRow: {rawRow.Count}, TableColumns: {tableColumnCount})");
        }

        if (hasColumns && rawRow.Count != _model.Columns.Count)
        {
            throw new BindingException("The number of values provided in a row must be the same as " +
                                "the number of columns provided inside the parenthesis after the table name attribute.");
        }
    }

    private Dictionary<string, object?> BuildInputRow(
        List<string> rawRow,
        List<Column> tableColumns,
        bool hasColumns)
    {
        var inputRow = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        if (!hasColumns)
        {
            for (int i = 0; i < tableColumns.Count; i++)
            {
                inputRow[tableColumns[i].Name] = rawRow[i].Replace("'", "");
            }

            return inputRow;
        }

        for (int i = 0; i < _model.Columns.Count; i++)
        {
            inputRow[_model.Columns[i]] = rawRow[i].Replace("'", "");
        }

        return inputRow;
    }
}
