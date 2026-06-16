using DataVo.Core.Enums;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.DDL;

internal static class ColumnDefinitionParser
{
    public static DataTypes ParseType(string typeStr)
    {
        string t = typeStr.ToLowerInvariant();
        if (t.Contains("int")) return DataTypes.Int;
        if (t.Contains("float")) return DataTypes.Float;
        if (t.Contains("bit")) return DataTypes.Bit;
        if (t.Contains("date")) return DataTypes.Date;
        if (t.Contains("vector")) return DataTypes.Vector;
        return DataTypes.Varchar;
    }

    public static int ParseLength(string typeStr)
    {
        int start = typeStr.IndexOf('(');
        if (start > -1 && int.TryParse(typeStr[(start + 1)..].TrimEnd(')'), out int len))
        {
            return len;
        }

        return 0;
    }

    public static string? EvaluateDefaultExpression(ExpressionNode? expr, string operationName)
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

        if (expr is ColumnRefNode colRef
            && (colRef.Column.Equals("true", StringComparison.OrdinalIgnoreCase)
                || colRef.Column.Equals("false", StringComparison.OrdinalIgnoreCase)))
        {
            return colRef.Column.ToLowerInvariant();
        }

        throw new ParserException($"{operationName} default must be a constant literal value.");
    }

    public static Column ToColumn(Field field)
    {
        return new Column
        {
            Name = field.Name,
            Type = field.Type.ToString().ToUpperInvariant(),
            Length = field.Length,
            DefaultValue = field.DefaultValue
        };
    }
}
