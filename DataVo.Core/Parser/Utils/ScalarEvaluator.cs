using System.Globalization;
using DataVo.Core.Exceptions;
using DataVo.Core.Parser.AST;
using DataVo.Core.Enums;

namespace DataVo.Core.Parser.Utils;

internal static class ScalarEvaluator
{
    public static object? Evaluate(ExpressionNode expression, Dictionary<string, object?> row)
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
            object? left = Evaluate(binary.Left, row);
            object? right = Evaluate(binary.Right, row);

            if (left == null || right == null) return null; // SQL NULL propagation

            return ApplyArithmetic(binary.Operator, left, right);
        }

        throw new EvaluationException($"Expression type {expression.GetType().Name} not supported in scalar evaluation");
    }

    /// <summary>
    /// Applies a binary arithmetic operator to two boxed operands without the DLR. Reproduces the prior
    /// <c>dynamic</c> semantics: <c>+</c> concatenates when either side is a string; numeric operands are
    /// promoted to the widest type involved (decimal &gt; double &gt; long &gt; int); integer division
    /// truncates. Native-AOT safe (no Microsoft.CSharp).
    /// </summary>
    private static object ApplyArithmetic(string op, object left, object right)
    {
        // '+' with a string operand performs concatenation, matching the previous dynamic '+'.
        if (op == Operators.ADD && (left is string || right is string))
        {
            return string.Concat(left, right);
        }

        if (left is decimal || right is decimal)
        {
            decimal l = Convert.ToDecimal(left, CultureInfo.InvariantCulture);
            decimal r = Convert.ToDecimal(right, CultureInfo.InvariantCulture);
            return op switch
            {
                Operators.ADD => l + r,
                Operators.SUBTRACT => l - r,
                Operators.MUL => l * r,
                Operators.DIVIDE => l / r,
                _ => throw UnsupportedOperator(op),
            };
        }

        if (left is double or float || right is double or float)
        {
            double l = Convert.ToDouble(left, CultureInfo.InvariantCulture);
            double r = Convert.ToDouble(right, CultureInfo.InvariantCulture);
            return op switch
            {
                Operators.ADD => l + r,
                Operators.SUBTRACT => l - r,
                Operators.MUL => l * r,
                Operators.DIVIDE => l / r,
                _ => throw UnsupportedOperator(op),
            };
        }

        if (left is long || right is long)
        {
            long l = Convert.ToInt64(left, CultureInfo.InvariantCulture);
            long r = Convert.ToInt64(right, CultureInfo.InvariantCulture);
            return op switch
            {
                Operators.ADD => l + r,
                Operators.SUBTRACT => l - r,
                Operators.MUL => l * r,
                Operators.DIVIDE => l / r,
                _ => throw UnsupportedOperator(op),
            };
        }

        if (IsIntegral(left) && IsIntegral(right))
        {
            int l = Convert.ToInt32(left, CultureInfo.InvariantCulture);
            int r = Convert.ToInt32(right, CultureInfo.InvariantCulture);
            return op switch
            {
                Operators.ADD => l + r,
                Operators.SUBTRACT => l - r,
                Operators.MUL => l * r,
                Operators.DIVIDE => l / r,
                _ => throw UnsupportedOperator(op),
            };
        }

        throw new EvaluationException(
            $"Cannot apply operator {op} to operands of type {left.GetType().Name} and {right.GetType().Name}");
    }

    private static bool IsIntegral(object value) =>
        value is int or short or byte or sbyte or ushort or uint;

    private static EvaluationException UnsupportedOperator(string op) =>
        new($"Operator {op} not supported in SET clause scalar evaluation");
}
