using DataVo.Core.BTree;
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

            Locks.AcquireWriteLock(databaseName, tableName);

            try
            {
                var existingRows = Context.GetTableContents(tableName, databaseName)
                    .OrderBy(row => row.Key)
                    .Select(row => new Dictionary<string, dynamic>(row.Value))
                    .ToList();

                var indexes = Catalog.GetTableIndexes(tableName, databaseName);

                Catalog.DropColumn(tableName, databaseName, columnName);

                foreach (var row in existingRows)
                {
                    row.Remove(columnName);
                }

                RewriteTable(tableName, databaseName, existingRows, indexes);
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
            Messages.Add($"Error: {ex.Message}");
        }
    }

    private void ValidateSupportedShape(string databaseName, string tableName, string columnName)
    {
        if (!Catalog.TableExists(tableName, databaseName))
        {
            throw new Exception($"Table {tableName} does not exist in database {databaseName}!");
        }

        var columns = Catalog.GetTableColumns(tableName, databaseName);
        if (!columns.Any(column => column.Name == columnName))
        {
            throw new Exception($"Column {columnName} does not exist in table {tableName}!");
        }

        if (columns.Count <= 1)
        {
            throw new Exception("ALTER TABLE DROP COLUMN cannot remove the last remaining column.");
        }

        if (Catalog.GetTablePrimaryKeys(tableName, databaseName).Contains(columnName))
        {
            throw new Exception("ALTER TABLE DROP COLUMN cannot drop a PRIMARY KEY column in this version.");
        }

        if (Catalog.GetTableUniqueKeys(tableName, databaseName).Contains(columnName))
        {
            throw new Exception("ALTER TABLE DROP COLUMN cannot drop a UNIQUE column in this version.");
        }

        if (Catalog.GetTableForeignKeys(tableName, databaseName).Any(fk => fk.AttributeName == columnName))
        {
            throw new Exception("ALTER TABLE DROP COLUMN cannot drop a FOREIGN KEY column in this version.");
        }

        if (Catalog.GetChildForeignKeys(tableName, databaseName).Any(fk => fk.ParentColumn == columnName))
        {
            throw new Exception("ALTER TABLE DROP COLUMN cannot drop a referenced column in this version.");
        }

        if (Catalog.GetTableIndexedColumns(tableName, databaseName).ContainsKey(columnName))
        {
            throw new Exception("ALTER TABLE DROP COLUMN cannot drop an indexed column in this version.");
        }
    }

    private void RewriteTable(string tableName, string databaseName, List<Dictionary<string, dynamic>> rows, List<IndexFile> indexes)
    {
        Context.DropTable(tableName, databaseName);
        Context.CreateTable(tableName, databaseName);

        foreach (var index in indexes)
        {
            Indexes.DropIndex(index.IndexFileName, tableName, databaseName);
        }

        List<long> newRowIds = Context.InsertIntoTable(rows, tableName, databaseName);

        foreach (var index in indexes)
        {
            Dictionary<string, List<long>> indexData = [];

            for (int i = 0; i < rows.Count; i++)
            {
                string key = IndexKeyEncoder.BuildKeyString(rows[i], index.AttributeNames);
                if (!indexData.TryGetValue(key, out var ids))
                {
                    ids = [];
                    indexData[key] = ids;
                }

                ids.Add(newRowIds[i]);
            }

            Indexes.CreateIndex(indexData, index.IndexFileName, tableName, databaseName);
        }
    }
}