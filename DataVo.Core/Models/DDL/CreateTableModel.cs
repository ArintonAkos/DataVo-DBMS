using System.Text.RegularExpressions;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.Enums;

namespace DataVo.Core.Models.DDL;

/// <summary>
/// Represents the normalized model for a CREATE TABLE statement.
/// </summary>
/// <param name="tableName">The table name.</param>
/// <param name="fields">The table field definitions.</param>
public class CreateTableModel(string tableName, List<Field> fields)
{
    /// <summary>
    /// Gets or sets the table name.
    /// </summary>
    public string TableName { get; set; } = tableName;

    /// <summary>
    /// Gets or sets the field definitions.
    /// </summary>
    public List<Field> Fields { get; set; } = fields;

    /// <summary>
    /// Gets primary key column names.
    /// </summary>
    public List<string> PrimaryKeys
    {
        get
        {
            return [.. Fields.FindAll(f => f.IsPrimaryKey == true).Select(f => f.Name)];
        }
    }

    /// <summary>
    /// Gets unique-constraint column names.
    /// </summary>
    public List<string> UniqueAttributes
    {
        get
        {
            return Fields.FindAll(f => f.IsUnique == true)
                .Select(f => f.Name)
                .ToList();
        }
    }


    /// <summary>
    /// Gets foreign key definitions.
    /// </summary>
    public List<ForeignKey> ForeignKeys
    {
        get
        {
            return Fields.FindAll(f => f.ForeignKey != null)
                .Select(f => f.ForeignKey!)
                .ToList();
        }
    }

    /// <summary>
    /// Builds a create-table model from CREATE TABLE AST.
    /// </summary>
    /// <param name="ast">The parsed CREATE TABLE statement.</param>
    /// <returns>The normalized create-table model.</returns>
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

        throw new EvaluationException($"DEFAULT expression must be a constant literal value. Actual type: {expr.GetType()}");
    }

    private static DataTypes ParseType(string typeStr)
    {
        string t = typeStr.ToLowerInvariant();
        if (t.Contains("guid") || t.Contains("uuid") || t.Contains("uniqueidentifier")) return DataTypes.Guid;
        if (t.Contains("int")) return DataTypes.Int;
        if (t.Contains("float")) return DataTypes.Float;
        if (t.Contains("bit")) return DataTypes.Bit;
        if (t.Contains("date")) return DataTypes.Date;
        if (t.Contains("vector")) return DataTypes.Vector;
        return DataTypes.Varchar;
    }

    private static int ParseLength(string typeStr)
    {
        int start = typeStr.IndexOf('(');
        if (start > -1 && int.TryParse(typeStr.Substring(start + 1).TrimEnd(')'), out int len))
            return len;
        return 0;
    }

    /// <summary>
    /// Converts the model into catalog table metadata.
    /// </summary>
    /// <returns>The catalog table definition.</returns>
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
