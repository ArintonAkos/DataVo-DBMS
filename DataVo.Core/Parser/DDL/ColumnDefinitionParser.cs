using DataVo.Core.Enums;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.DDL;

internal static class ColumnDefinitionParser
{
    /// <summary>
    /// Resolves a declared SQL type. Every accepted name is stored as what it says; a type DataVo cannot
    /// store faithfully is an error rather than a silent fallback to text.
    /// </summary>
    public static DataTypes ParseType(string typeStr)
    {
        int parenthesis = typeStr.IndexOf('(');
        string name = (parenthesis < 0 ? typeStr : typeStr[..parenthesis]).Trim().ToUpperInvariant();
        return name switch
        {
            "INT" or "INTEGER" or "SMALLINT" or "TINYINT" or "MEDIUMINT" => DataTypes.Int,
            "BIGINT" => DataTypes.BigInt,
            "FLOAT" or "REAL" => DataTypes.Float,
            "DOUBLE" => DataTypes.Double,
            "BIT" or "BOOL" or "BOOLEAN" => DataTypes.Bit,
            "DATE" => DataTypes.Date,
            "GUID" or "UUID" or "UNIQUEIDENTIFIER" => DataTypes.Guid,
            "VARCHAR" or "NVARCHAR" or "CHAR" or "NCHAR" or "TEXT" or "STRING" => DataTypes.Varchar,
            "VECTOR" => DataTypes.Vector,
            _ => throw new ParserException(UnsupportedTypeMessage(name)),
        };
    }

    private static string UnsupportedTypeMessage(string name)
    {
        string advice = name switch
        {
            "LONG" => " Use BIGINT for a 64-bit integer.",
            "DECIMAL" or "NUMERIC" or "MONEY" => " Use DOUBLE for an approximate number, or VARCHAR to keep the exact digits.",
            "DATETIME" or "TIMESTAMP" or "TIME" => " DATE keeps only the date; store a point in time in a BIGINT.",
            _ => string.Empty,
        };

        return $"Column type '{name}' is not supported.{advice} Supported types: INT, BIGINT, FLOAT, DOUBLE, BIT, DATE, GUID, VARCHAR, VECTOR.";
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
