using DataVo.Core.BTree;
using DataVo.Core.Enums;
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
                    .Select(row => new Dictionary<string, dynamic>(row.Value))
                    .ToList();

                var indexes = Catalog.GetTableIndexes(tableName, databaseName);
                Field field = ToField(ast.Column, tableName);
                dynamic? defaultValue = ToColumn(field).ParsedDefaultValue;

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
            throw new Exception($"Table {tableName} does not exist in database {databaseName}!");
        }

        if (Catalog.GetTableColumns(tableName, databaseName).Any(column => column.Name == ast.Column.ColumnName.Name))
        {
            throw new Exception($"Column {ast.Column.ColumnName.Name} already exists in table {tableName}!");
        }

        if (ast.Column.IsPrimaryKey || ast.Column.IsUnique || ast.Column.ReferencesTable != null)
        {
            throw new Exception("ALTER TABLE ADD COLUMN currently supports only nullable/default columns without PK, UNIQUE, or FOREIGN KEY constraints.");
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

    private static Field ToField(ColumnDefinitionNode column, string tableName)
    {
        return new Field
        {
            Name = column.ColumnName.Name,
            Type = ParseType(column.DataType),
            Length = ParseLength(column.DataType),
            Table = tableName,
            IsPrimaryKey = false,
            IsUnique = false,
            IsNull = -1,
            DefaultValue = EvaluateDefaultExpression(column.DefaultExpression),
            ForeignKey = null
        };
    }

    private static Column ToColumn(Field field)
    {
        return new Column
        {
            Name = field.Name,
            Type = field.Type.ToString().ToUpperInvariant(),
            Length = field.Length,
            DefaultValue = field.DefaultValue
        };
    }

    private static string? EvaluateDefaultExpression(ExpressionNode? expr)
    {
        if (expr == null)
        {
            return null;
        }

        if (expr is NullLiteralNode)
        {
            return "NULL";
        }

        if (expr is LiteralNode literal)
        {
            string value = literal.Value?.ToString() ?? "NULL";
            if (value.StartsWith("'") && value.EndsWith("'"))
            {
                value = value[1..^1];
            }

            return value;
        }

        if (expr is ColumnRefNode colRef &&
            (colRef.Column.Equals("true", StringComparison.OrdinalIgnoreCase)
             || colRef.Column.Equals("false", StringComparison.OrdinalIgnoreCase)))
        {
            return colRef.Column.ToLowerInvariant();
        }

        throw new Exception("ALTER TABLE ADD COLUMN default must be a constant literal value.");
    }

    private static DataTypes ParseType(string typeStr)
    {
        string t = typeStr.ToLowerInvariant();
        if (t.Contains("int")) return DataTypes.Int;
        if (t.Contains("float")) return DataTypes.Float;
        if (t.Contains("bit")) return DataTypes.Bit;
        if (t.Contains("date")) return DataTypes.Date;
        return DataTypes.Varchar;
    }

    private static int ParseLength(string typeStr)
    {
        int start = typeStr.IndexOf('(');
        if (start > -1 && int.TryParse(typeStr[(start + 1)..].TrimEnd(')'), out int len))
        {
            return len;
        }

        return 0;
    }
}