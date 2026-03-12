using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.Utils;

internal static class AggregateExpressionFormatter
{
    public static string BuildHeader(AggregateExpressionNode aggregate)
    {
        string functionName = aggregate.FunctionName.ToUpperInvariant();

        if (aggregate.IsStar)
        {
            return $"{functionName}(*)";
        }

        string argument = aggregate.Argument == null ? string.Empty : FormatExpression(aggregate.Argument);
        return $"{functionName}({argument})";
    }

    public static string FormatExpression(ExpressionNode expression)
    {
        return expression switch
        {
            ResolvedColumnRefNode resolved => $"{resolved.TableName}.{resolved.Column}",
            ColumnRefNode column when string.IsNullOrWhiteSpace(column.TableOrAlias) => column.Column,
            ColumnRefNode column => $"{column.TableOrAlias}.{column.Column}",
            LiteralNode literal when literal.Value is string s => s,
            NullLiteralNode => "NULL",
            LiteralNode literal => literal.Value?.ToString() ?? "NULL",
            BinaryExpressionNode binary => $"{FormatExpression(binary.Left)} {binary.Operator} {FormatExpression(binary.Right)}",
            AggregateExpressionNode aggregate => BuildHeader(aggregate),
            _ => expression.ToString() ?? string.Empty
        };
    }
}
