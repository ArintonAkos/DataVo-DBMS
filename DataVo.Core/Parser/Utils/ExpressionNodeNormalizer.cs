using DataVo.Core.Parser.AST;
using DataVo.Core.Enums;

namespace DataVo.Core.Parser.Utils;

internal static class ExpressionNodeNormalizer
{
    public static BinaryExpressionNode NormalizeComparisonNode(BinaryExpressionNode node)
    {
        if (node.Left is LiteralNode && node.Right is ResolvedColumnRefNode)
        {
            string normalizedOperator = node.Operator switch
            {
                Operators.LESS_THAN => Operators.GREATER_THAN,
                Operators.GREATER_THAN => Operators.LESS_THAN,
                Operators.LESS_THAN_OR_EQUAL_TO => Operators.GREATER_THAN_OR_EQUAL_TO,
                Operators.GREATER_THAN_OR_EQUAL_TO => Operators.LESS_THAN_OR_EQUAL_TO,
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
