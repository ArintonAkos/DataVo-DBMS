using DataVo.Core.Constants;
using DataVo.Core.Parser.AST;
using DataVo.Core.Enums;

namespace DataVo.Core.Parser.Utils;

internal static class ScalarEvaluator
{
    public static dynamic? Evaluate(ExpressionNode expression, Dictionary<string, dynamic> row)
    {
        if (expression is LiteralNode literal)
        {
            return literal.Value;
        }

        if (expression is ColumnRefNode colRef)
        {
            if (row.TryGetValue(colRef.Column, out var value))
                return value;
            return null; // or throw "Column not found"
        }

        if (expression is ResolvedColumnRefNode resolvedCol)
        {
            if (row.TryGetValue(resolvedCol.Column, out var value))
                return value;
            return null;
        }

        if (expression is BinaryExpressionNode binary)
        {
            dynamic? left = Evaluate(binary.Left, row);
            dynamic? right = Evaluate(binary.Right, row);

            if (left == null || right == null) return null; // SQL NULL propagation

            return binary.Operator switch
            {
                Operators.ADD => left + right,
                Operators.SUBTRACT => left - right,
                Operators.MUL => left * right,
                Operators.DIVIDE => left / right,
                _ => throw new Exception($"Operator {binary.Operator} not supported in SET clause scalar evaluation")
            };
        }

        throw new Exception($"Expression type {expression.GetType().Name} not supported in scalar evaluation");
    }
}
