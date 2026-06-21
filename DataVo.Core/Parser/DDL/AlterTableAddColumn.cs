using DataVo.Core.BTree;
using DataVo.Core.Exceptions;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.DDL;

internal class AlterTableAddColumn(AlterTableAddColumnStatement ast) : BaseDbAction
{
    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = GetDatabaseName(session);
            string tableName = ast.TableName.Name;

            ValidateSupportedShape(databaseName, tableName);

            Locks.AcquireWriteLock(databaseName, tableName);

            try
            {
                var existingRows = Context.GetTableContents(tableName, databaseName)
                    .OrderBy(row => row.Key)
                    .Select(row => new Dictionary<string, object?>(row.Value))
                    .ToList();

                var indexes = Catalog.GetTableIndexes(tableName, databaseName);
                Field field = ToField(ast.Column, tableName);
                object? defaultValue = ColumnDefinitionParser.ToColumn(field).ParsedDefaultValue;

                Catalog.AddColumn(tableName, databaseName, field);

                foreach (var row in existingRows)
                {
                    row[field.Name] = defaultValue!;
                }

                RewriteTable(tableName, databaseName, existingRows, indexes);
            }
            finally
            {
                Locks.ReleaseWriteLock(databaseName, tableName);
            }

            Messages.Add($"Table {tableName} altered successfully. Added column {ast.Column.ColumnName.Name}.");
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add($"Error: {ex.Message}");
        }
    }

    private void ValidateSupportedShape(string databaseName, string tableName)
    {
        if (!Catalog.TableExists(tableName, databaseName))
        {
            throw new CatalogException($"Table {tableName} does not exist in database {databaseName}!");
        }

        if (Catalog.GetTableColumns(tableName, databaseName).Any(column => column.Name == ast.Column.ColumnName.Name))
        {
            throw new CatalogException($"Column {ast.Column.ColumnName.Name} already exists in table {tableName}!");
        }

        if (ast.Column.IsPrimaryKey || ast.Column.IsUnique || ast.Column.ReferencesTable != null)
        {
            throw new CatalogException("ALTER TABLE ADD COLUMN currently supports only nullable/default columns without PK, UNIQUE, or FOREIGN KEY constraints.");
        }
    }

    private void RewriteTable(string tableName, string databaseName, List<Dictionary<string, object?>> rows, List<IndexFile> indexes)
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

    private static Field ToField(ColumnDefinitionNode column, string tableName)
    {
        return new Field
        {
            Name = column.ColumnName.Name,
            Type = ColumnDefinitionParser.ParseType(column.DataType),
            Length = ColumnDefinitionParser.ParseLength(column.DataType),
            Table = tableName,
            IsPrimaryKey = false,
            IsUnique = false,
            IsNull = -1,
            DefaultValue = ColumnDefinitionParser.EvaluateDefaultExpression(column.DefaultExpression, "ALTER TABLE ADD COLUMN"),
            ForeignKey = null
        };
    }
}