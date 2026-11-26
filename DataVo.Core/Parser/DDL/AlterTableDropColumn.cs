using DataVo.Core.StorageEngine;
using DataVo.Core.Exceptions;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.DDL;

internal class AlterTableDropColumn(AlterTableDropColumnStatement ast) : BaseDbAction
{
    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = GetDatabaseName(session);
            string tableName = ast.TableName.Name;
            string columnName = ast.ColumnName.Name;

            ValidateSupportedShape(databaseName, tableName, columnName);
            columnName = TableRewrite.StoredColumnName(Catalog, databaseName, tableName, columnName);

            Locks.AcquireWriteLock(databaseName, tableName);

            try
            {
                var existingRows = Context.GetTableContents(tableName, databaseName)
                    .OrderBy(row => row.Key)
                    .Select(row => new Dictionary<string, object?>(row.Value))
                    .ToList();

                var indexes = Catalog.GetTableIndexes(tableName, databaseName);

                Catalog.DropColumn(tableName, databaseName, columnName);

                foreach (var row in existingRows)
                {
                    row.Remove(columnName);
                }

                TableRewrite.ReplaceRows(Engine, databaseName, tableName, existingRows, indexes);
            }
            finally
            {
                Locks.ReleaseWriteLock(databaseName, tableName);
            }

            Messages.Add($"Table {tableName} altered successfully. Dropped column {columnName}.");
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            AddError(ex);
        }
    }

    private void ValidateSupportedShape(string databaseName, string tableName, string columnName)
    {
        if (!Catalog.TableExists(tableName, databaseName))
        {
            throw new CatalogException($"Table {tableName} does not exist in database {databaseName}!");
        }

        var columns = Catalog.GetTableColumns(tableName, databaseName);
        if (!columns.Any(column => TableRewrite.SameColumn(column.Name, columnName)))
        {
            throw new CatalogException($"Column {columnName} does not exist in table {tableName}!");
        }

        if (columns.Count <= 1)
        {
            throw new CatalogException("ALTER TABLE DROP COLUMN cannot remove the last remaining column.");
        }

        if (TableRewrite.ContainsColumn(Catalog.GetTablePrimaryKeys(tableName, databaseName), columnName))
        {
            throw new CatalogException("ALTER TABLE DROP COLUMN cannot drop a PRIMARY KEY column in this version.");
        }

        if (TableRewrite.ContainsColumn(Catalog.GetTableUniqueKeys(tableName, databaseName), columnName))
        {
            throw new CatalogException("ALTER TABLE DROP COLUMN cannot drop a UNIQUE column in this version.");
        }

        if (Catalog.GetTableForeignKeys(tableName, databaseName).Any(fk => TableRewrite.SameColumn(fk.AttributeName, columnName)))
        {
            throw new CatalogException("ALTER TABLE DROP COLUMN cannot drop a FOREIGN KEY column in this version.");
        }

        if (Catalog.GetChildForeignKeys(tableName, databaseName).Any(fk => TableRewrite.SameColumn(fk.ParentColumn, columnName)))
        {
            throw new CatalogException("ALTER TABLE DROP COLUMN cannot drop a referenced column in this version.");
        }

        if (TableRewrite.IsIndexedColumn(Catalog, databaseName, tableName, columnName))
        {
            throw new CatalogException("ALTER TABLE DROP COLUMN cannot drop an indexed column in this version.");
        }
    }

}