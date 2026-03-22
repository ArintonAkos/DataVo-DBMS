using System.Globalization;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Types;
using DataVo.Core.Parser.Utils;
using DataVo.Core.Enums;
using DataVo.Core.Utils;

namespace DataVo.Core.Parser.Statements.Mechanism;

internal static class ExpressionEvaluator
{
    public static object? Evaluate(ExpressionNode node, JoinedRow row, Func<ColumnRefNode, JoinedRow, object?> resolveColumn, Func<AggregateExpressionNode, JoinedRow, object?> resolveAggregate)
    {
        if (node is LiteralNode lit) return lit.Value;
        if (node is NullLiteralNode) return null;
        if (node is ResolvedColumnRefNode resolved)
        {
            var col = new ColumnRefNode { TableOrAlias = resolved.TableName, Column = resolved.Column };
            return resolveColumn(col, row);
        }
        if (node is ColumnRefNode colRef)
        {
            return resolveColumn(colRef, row);
        }
        if (node is AggregateExpressionNode agg)
        {
            return resolveAggregate(agg, row);
        }
        if (node is ScalarFunctionExpressionNode scalarFunction)
        {
            return EvaluateScalarFunction(scalarFunction, row, resolveColumn, resolveAggregate);
        }
        if (node is BinaryExpressionNode bin)
        {
            object? left = Evaluate(bin.Left, row, resolveColumn, resolveAggregate);
            object? right = Evaluate(bin.Right, row, resolveColumn, resolveAggregate);

            switch (bin.Operator)
            {
                case "+":
                case "ADD":
                    return ApplyNumericOp(left, right, (a, b) => a + b, (a, b) => string.Concat(a, b));
                case "-":
                case "SUB":
                    return ApplyNumericOp(left, right, (a, b) => a - b);
                case "*":
                case "MUL":
                    return ApplyNumericOp(left, right, (a, b) => a * b);
                case "/":
                case "DIV":
                    return ApplyNumericOp(left, right, (a, b) => a / b);
                case Operators.VECTOR_DISTANCE_L2:
                    return EvaluateVectorDistance(bin, left, right, useCosine: false);
                case Operators.VECTOR_DISTANCE_COSINE:
                    return EvaluateVectorDistance(bin, left, right, useCosine: true);
                case "=":
                case "==":
                case "EQ":
                    return ExpressionValueComparer.AreEqual(left, right, trimQuotedStrings: true, useNumericTolerance: true);
                case "!=":
                case "NEQ":
                    return !ExpressionValueComparer.AreEqual(left, right, trimQuotedStrings: true, useNumericTolerance: true);
                case ">":
                    return CompareToInt(left, right) > 0;
                case "<":
                    return CompareToInt(left, right) < 0;
                case ">=":
                    return CompareToInt(left, right) >= 0;
                case "<=":
                    return CompareToInt(left, right) <= 0;
                case "AND":
                case "and":
                    return ToBool(left) && ToBool(right);
                case "OR":
                case "or":
                    return ToBool(left) || ToBool(right);
                default:
                    throw new Exception($"Unsupported operator in expression evaluator: {bin.Operator}");
            }
        }

        throw new Exception($"Unsupported expression node type: {node.GetType().Name}");
    }

    private static double EvaluateVectorDistance(BinaryExpressionNode node, object? left, object? right, bool useCosine)
    {
        bool leftFromColumn = node.Left is ColumnRefNode || node.Left is ResolvedColumnRefNode;
        bool rightFromColumn = node.Right is ColumnRefNode || node.Right is ResolvedColumnRefNode;

        if (!VectorParser.TryCoerceToVector(left, out float[] leftVector))
        {
            string side = leftFromColumn ? "left VECTOR column" : "left vector operand";
            throw new Exception($"Vector operator '{node.Operator}' requires {side}.");
        }

        if (!VectorParser.TryCoerceToVector(right, out float[] rightVector))
        {
            string side = rightFromColumn ? "right VECTOR column" : "right vector operand";
            throw new Exception($"Vector operator '{node.Operator}' requires {side}.");
        }

        if (leftVector.Length != rightVector.Length)
        {
            throw new Exception($"Vector operator '{node.Operator}' requires matching dimensions ({leftVector.Length} != {rightVector.Length}).");
        }

        return useCosine
            ? VectorParser.CosineDistance(leftVector, rightVector)
            : VectorParser.EuclideanDistance(leftVector, rightVector);
    }

    private static object? EvaluateScalarFunction(
        ScalarFunctionExpressionNode scalarFunction,
        JoinedRow row,
        Func<ColumnRefNode, JoinedRow, object?> resolveColumn,
        Func<AggregateExpressionNode, JoinedRow, object?> resolveAggregate)
    {
        if (scalarFunction.Arguments.Count != 1)
        {
            throw new Exception($"Scalar function '{scalarFunction.FunctionName}' expects exactly one argument.");
        }

        object? value = Evaluate(scalarFunction.Arguments[0], row, resolveColumn, resolveAggregate);

        if (value == null)
        {
            return null;
        }

        string normalized = scalarFunction.FunctionName.ToUpperInvariant();
        string input = Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;

        return normalized switch
        {
            "LOWER" => input.ToLowerInvariant(),
            "UPPER" => input.ToUpperInvariant(),
            _ => throw new Exception($"Unsupported scalar function: {scalarFunction.FunctionName}")
        };
    }

    private static bool ToBool(object? v)
    {
        if (v == null) return false;
        if (v is bool b) return b;
        if (v is string s && bool.TryParse(s, out var sb)) return sb;
        if (v is int i) return i != 0;
        if (v is long l) return l != 0L;
        if (v is double d) return Math.Abs(d) > double.Epsilon;
        return false;
    }

    private static int CompareToInt(object? left, object? right)
    {
        if (left == null || right == null) throw new Exception("Cannot compare null values");
        return ExpressionValueComparer.Compare(left, right, trimQuotedStrings: true);
    }

    private static object? ApplyNumericOp(object? left, object? right, Func<double, double, double> op, Func<object, object, object>? stringConcat = null)
    {
        if (left == null || right == null) return null;

        if (left is string || right is string)
        {
            if (stringConcat != null) return stringConcat(left, right);
            throw new Exception("Cannot apply numeric operator to string operands");
        }

        double l = Convert.ToDouble(left, CultureInfo.InvariantCulture);
        double r = Convert.ToDouble(right, CultureInfo.InvariantCulture);

        double res = op(l, r);

        // If both inputs were integral, return long when possible
        if ((left is int || left is long) && (right is int || right is long) && Math.Abs(res % 1) < double.Epsilon)
        {
            return (long)res;
        }

        return res;
    }
}
