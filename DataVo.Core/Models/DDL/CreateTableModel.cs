using System.Text.RegularExpressions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.Enums;

namespace DataVo.Core.Models.DDL;

public class CreateTableModel(string tableName, List<Field> fields)
{
    public string TableName { get; set; } = tableName;

    public List<Field> Fields { get; set; } = fields;

    public List<string> PrimaryKeys
    {
        get
        {
            return [.. Fields.FindAll(f => f.IsPrimaryKey == true).Select(f => f.Name)];
        }
    }

    public List<string> UniqueAttributes
    {
        get
        {
            return Fields.FindAll(f => f.IsUnique == true)
                .Select(f => f.Name)
                .ToList();
        }
    }


    public List<ForeignKey> ForeignKeys
    {
        get
        {
            return Fields.FindAll(f => f.ForeignKey != null)
                .Select(f => f.ForeignKey!)
                .ToList();
        }
    }

    public static CreateTableModel FromAst(CreateTableStatement ast)
    {
        string tableName = ast.TableName.Name;
        List<Field> fields = ast.Columns.Select(c => new Field
        {
            Name = c.ColumnName.Name,
            Type = ParseType(c.DataType),
            Length = ParseLength(c.DataType),
            Table = tableName,
            IsPrimaryKey = c.IsPrimaryKey,
            IsUnique = c.IsUnique,
            IsNull = -1,
            DefaultValue = c.DefaultExpression != null ? EvaluateDefaultExpression(c.DefaultExpression) : null,
            ForeignKey = c.ReferencesTable != null ? new ForeignKey { AttributeName = c.ColumnName.Name, References = [new Reference { ReferenceTableName = c.ReferencesTable.Name, ReferenceAttributeName = c.ReferencesColumn!.Name }], OnDeleteAction = c.OnDeleteAction } : null
        }).ToList();
        return new CreateTableModel(tableName, fields);
    }

    private static string EvaluateDefaultExpression(ExpressionNode expr)
    {
        // Default values must be static literals when creating the table
        if (expr is NullLiteralNode) return "NULL";
        
        if (expr is LiteralNode literal)
        {
            string value = literal.Value?.ToString() ?? "NULL";
            if (value.StartsWith("'") && value.EndsWith("'"))
            {
                value = value.Substring(1, value.Length - 2);
            }
            return value;
        }
        else if (expr is ColumnRefNode colRef)
        {
            // For booleans, true/false are parsed as identifiers (ColumnRefNode)
            if (colRef.Column.Equals("true", StringComparison.OrdinalIgnoreCase) || 
                colRef.Column.Equals("false", StringComparison.OrdinalIgnoreCase))
            {
                return colRef.Column.ToLowerInvariant();
            }
        }
        
        throw new Exception($"DEFAULT expression must be a constant literal value. Actual type: {expr.GetType()}");
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
        if (start > -1 && int.TryParse(typeStr.Substring(start + 1).TrimEnd(')'), out int len))
            return len;
        return 0;
    }

    public Table ToTable() =>
        new()
        {
            TableName = TableName,
            Fields = Fields,
            PrimaryKeys = PrimaryKeys,
            UniqueAttributes = UniqueAttributes,
            ForeignKeys = ForeignKeys,
            IndexFiles = [],
        };
}