using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.Utils;

internal static class ExpressionNodeNormalizer
{
    public static BinaryExpressionNode NormalizeComparisonNode(BinaryExpressionNode node)
    {
        if (node.Left is LiteralNode && node.Right is ResolvedColumnRefNode)
        {
            string normalizedOperator = node.Operator switch
            {
                "<" => ">",
                ">" => "<",
                "<=" => ">=",
                ">=" => "<=",
                _ => node.Operator
            };

            return new BinaryExpressionNode
            {
                Operator = normalizedOperator,
                Left = node.Right,
                Right = node.Left
            };
        }

        return node;
    }
}
