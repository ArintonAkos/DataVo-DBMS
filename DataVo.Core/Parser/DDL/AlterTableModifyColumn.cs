using System.Globalization;
using DataVo.Core.BTree;
using DataVo.Core.Exceptions;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.DDL;

internal class AlterTableModifyColumn(AlterTableModifyColumnStatement ast) : BaseDbAction
{
    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = GetDatabaseName(session);
            string tableName = ast.TableName.Name;
            string columnName = ast.Column.ColumnName.Name;

            ValidateSupportedShape(databaseName, tableName, columnName);

            Locks.AcquireWriteLock(databaseName, tableName);

            try
            {
                var existingRows = Context.GetTableContents(tableName, databaseName)
                    .OrderBy(row => row.Key)
                    .Select(row => new Dictionary<string, dynamic>(row.Value))
                    .ToList();

                var indexes = Catalog.GetTableIndexes(tableName, databaseName);
                var existingColumn = Catalog.GetTableColumn(tableName, databaseName, columnName);
                Field field = ToField(ast.Column, tableName, existingColumn);
                ValidateDefault(field);

                foreach (var row in existingRows)
                {
                    row[columnName] = ConvertExistingValue(row[columnName], field);
                }

                Catalog.ModifyColumn(tableName, databaseName, field);
                RewriteTable(tableName, databaseName, existingRows, indexes);
            }
            finally
            {
                Locks.ReleaseWriteLock(databaseName, tableName);
            }

            Messages.Add($"Table {tableName} altered successfully. Modified column {columnName}.");
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
            throw new CatalogException($"Table {tableName} does not exist in database {databaseName}!");
        }

        if (!Catalog.GetTableColumns(tableName, databaseName).Any(column => column.Name == columnName))
        {
            throw new CatalogException($"Column {columnName} does not exist in table {tableName}!");
        }

        if (ast.Column.IsPrimaryKey || ast.Column.IsUnique || ast.Column.ReferencesTable != null)
        {
            throw new CatalogException("ALTER TABLE MODIFY COLUMN currently supports only type/length/default changes without PK, UNIQUE, or FOREIGN KEY constraints.");
        }

        if (Catalog.GetTablePrimaryKeys(tableName, databaseName).Contains(columnName))
        {
            throw new CatalogException("ALTER TABLE MODIFY COLUMN cannot modify a PRIMARY KEY column in this version.");
        }

        if (Catalog.GetTableUniqueKeys(tableName, databaseName).Contains(columnName))
        {
            throw new CatalogException("ALTER TABLE MODIFY COLUMN cannot modify a UNIQUE column in this version.");
        }

        if (Catalog.GetTableForeignKeys(tableName, databaseName).Any(fk => fk.AttributeName == columnName))
        {
            throw new CatalogException("ALTER TABLE MODIFY COLUMN cannot modify a FOREIGN KEY column in this version.");
        }

        if (Catalog.GetChildForeignKeys(tableName, databaseName).Any(fk => fk.ParentColumn == columnName))
        {
            throw new CatalogException("ALTER TABLE MODIFY COLUMN cannot modify a referenced column in this version.");
        }

        if (Catalog.GetTableIndexedColumns(tableName, databaseName).ContainsKey(columnName))
        {
            throw new CatalogException("ALTER TABLE MODIFY COLUMN cannot modify an indexed column in this version.");
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

    private static Field ToField(ColumnDefinitionNode column, string tableName, Column existingColumn)
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
            DefaultValue = column.DefaultExpression != null
                ? ColumnDefinitionParser.EvaluateDefaultExpression(column.DefaultExpression, "ALTER TABLE MODIFY COLUMN")
                : existingColumn.DefaultValue,
            ForeignKey = null
        };
    }

    private static void ValidateDefault(Field field)
    {
        if (field.DefaultValue == null || field.DefaultValue.Equals("NULL", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        var column = ColumnDefinitionParser.ToColumn(field);
        if (column.ParsedDefaultValue == null)
        {
            throw new CatalogException($"ALTER TABLE MODIFY COLUMN default value for {field.Name} is incompatible with type {column.Type}.");
        }
    }

    private static dynamic? ConvertExistingValue(dynamic? value, Field field)
    {
        if (value == null)
        {
            return null;
        }

        var column = ColumnDefinitionParser.ToColumn(field);
        column.Value = ToRawValue(value);

        dynamic? parsedValue = column.ParsedValue;
        if (parsedValue == null)
        {
            throw new CatalogException($"ALTER TABLE MODIFY COLUMN cannot convert existing value '{column.Value}' in column {field.Name} to type {column.Type}.");
        }

        return parsedValue;
    }

    private static string ToRawValue(dynamic value)
    {
        return value switch
        {
            DateOnly dateOnly => dateOnly.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            DateTime dateTime => dateTime.ToString("O", CultureInfo.InvariantCulture),
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString() ?? "NULL"
        };
    }

}