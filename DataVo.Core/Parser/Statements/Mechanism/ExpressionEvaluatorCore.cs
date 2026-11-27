using System.Globalization;
using System.Security;
using DataVo.Core.Enums;
using DataVo.Core.Exceptions;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Utils;
using DataVo.Core.Constants;

namespace DataVo.Core.Parser.Statements.Mechanism;

/// <summary>
/// Generic recursive expression evaluator that dispatches comparison and logical operations
/// to specialized handlers implemented by derived evaluators.
/// </summary>
/// <typeparam name="TResult">The evaluator result type.</typeparam>
public abstract class ExpressionEvaluatorCore<TResult>
{
    /// <summary>
    /// Evaluates an expression tree and returns the derived evaluator result.
    /// </summary>
    /// <param name="root">The expression root node.</param>
    /// <returns>The evaluated result.</returns>
    public TResult Evaluate(ExpressionNode root)
    {
        if (root is ParameterNode unresolvedParam)
        {
            throw new BindingException($"Bind Error: Parameter '@{unresolvedParam.Name}' was not provided.");
        }

        if (root is LiteralNode literalNode)
        {
            return IsAlwaysTrueLiteral(literalNode)
                ? EvaluateTrueLiteral()
                : EvaluateFalseLiteral();
        }

        if (root is not BinaryExpressionNode binaryNode)
        {
            throw new EvaluationException("Invalid expression tree node type: expected BinaryExpressionNode or LiteralNode.");
        }

        if (binaryNode.Left is ParameterNode leftParam)
        {
            throw new BindingException($"Bind Error: Parameter '@{leftParam.Name}' was not provided.");
        }

        if (binaryNode.Right is ParameterNode rightParam)
        {
            throw new BindingException($"Bind Error: Parameter '@{rightParam.Name}' was not provided.");
        }

        bool isLogical = binaryNode.Operator == Operators.AND || binaryNode.Operator == Operators.OR;

        if (!isLogical)
        {
            var comparisonNode = ExpressionNodeNormalizer.NormalizeComparisonNode(binaryNode);

            if (TryGetVectorDistanceThresholdComparison(comparisonNode, out var distanceExpression, out var comparisonOperator, out var thresholdLiteral))
            {
                return HandleDistanceThresholdExpression(distanceExpression, comparisonOperator, thresholdLiteral);
            }

            if (comparisonNode.Left is ResolvedColumnRefNode && comparisonNode.Right is ResolvedColumnRefNode)
            {
                return HandleTwoColumnExpression(comparisonNode);
            }

            if (comparisonNode.Operator == Operators.EQUALS)
            {
                if (comparisonNode.Left is ResolvedColumnRefNode && comparisonNode.Right is LiteralNode)
                {
                    return HandleIndexableStatement(comparisonNode);
                }

                if (comparisonNode.Left is LiteralNode && comparisonNode.Right is LiteralNode)
                {
                    return HandleConstantExpression(comparisonNode);
                }
            }

            if (comparisonNode.Left is ResolvedColumnRefNode && comparisonNode.Right is LiteralNode)
            {
                return HandleNonIndexableStatement(comparisonNode);
            }

            if (comparisonNode.Left is LiteralNode && comparisonNode.Right is LiteralNode)
            {
                return HandleConstantExpression(comparisonNode);
            }
        }

        TResult leftResult = Evaluate(binaryNode.Left);
        TResult rightResult = Evaluate(binaryNode.Right);

        if (binaryNode.Operator == Operators.AND)
        {
            return And(leftResult, rightResult);
        }

        if (binaryNode.Operator == Operators.OR)
        {
            return Or(leftResult, rightResult);
        }

        throw new EvaluationException($"Invalid expression operator: {binaryNode.Operator}");
    }

    private static bool IsAlwaysTrueLiteral(LiteralNode literalNode)
    {
        if (literalNode.Value is bool b)
        {
            return b;
        }

        if (literalNode.Value is string s)
        {
            return s == SqlLiterals.TrueExpression;
        }

        return false;
    }

    /// <summary>
    /// Produces evaluator output for a literal that resolves to logical true.
    /// </summary>
    /// <returns>The evaluator-specific true result.</returns>
    protected abstract TResult EvaluateTrueLiteral();

    /// <summary>
    /// Produces evaluator output for a literal that resolves to logical false.
    /// </summary>
    /// <returns>The evaluator-specific false result.</returns>
    protected abstract TResult EvaluateFalseLiteral();

    /// <summary>
    /// Handles index-eligible comparison statements.
    /// </summary>
    /// <param name="root">The normalized comparison node.</param>
    /// <returns>The evaluator result.</returns>
    protected abstract TResult HandleIndexableStatement(BinaryExpressionNode root);

    /// <summary>
    /// Handles non-indexable comparison statements.
    /// </summary>
    /// <param name="root">The normalized comparison node.</param>
    /// <returns>The evaluator result.</returns>
    protected abstract TResult HandleNonIndexableStatement(BinaryExpressionNode root);

    /// <summary>
    /// Handles vector-distance threshold comparisons.
    /// </summary>
    /// <param name="distanceExpression">The vector-distance expression node.</param>
    /// <param name="comparisonOperator">The threshold comparison operator.</param>
    /// <param name="thresholdLiteral">The threshold literal value.</param>
    /// <returns>The evaluator result.</returns>
    protected abstract TResult HandleDistanceThresholdExpression(BinaryExpressionNode distanceExpression, string comparisonOperator, object? thresholdLiteral);

    /// <summary>
    /// Handles comparisons between two resolved columns.
    /// </summary>
    /// <param name="root">The normalized comparison node.</param>
    /// <returns>The evaluator result.</returns>
    protected abstract TResult HandleTwoColumnExpression(BinaryExpressionNode root);

    /// <summary>
    /// Handles constant-only comparison expressions.
    /// </summary>
    /// <param name="root">The normalized comparison node.</param>
    /// <returns>The evaluator result.</returns>
    protected abstract TResult HandleConstantExpression(BinaryExpressionNode root);

    /// <summary>
    /// Combines two partial results with logical AND semantics.
    /// </summary>
    /// <param name="leftResult">The left result.</param>
    /// <param name="rightResult">The right result.</param>
    /// <returns>The combined result.</returns>
    protected abstract TResult And(TResult leftResult, TResult rightResult);

    /// <summary>
    /// Combines two partial results with logical OR semantics.
    /// </summary>
    /// <param name="leftResult">The left result.</param>
    /// <param name="rightResult">The right result.</param>
    /// <returns>The combined result.</returns>
    protected abstract TResult Or(TResult leftResult, TResult rightResult);

    private static bool TryGetVectorDistanceThresholdComparison(
        BinaryExpressionNode comparisonNode,
        out BinaryExpressionNode distanceExpression,
        out string comparisonOperator,
        out object? thresholdLiteral)
    {
        distanceExpression = null!;
        comparisonOperator = string.Empty;
        thresholdLiteral = null;

        if (!IsRelationalComparisonOperator(comparisonNode.Operator))
        {
            return false;
        }

        if (comparisonNode.Left is BinaryExpressionNode leftDistance
            && IsVectorDistanceOperator(leftDistance.Operator)
            && comparisonNode.Right is LiteralNode rightLiteral)
        {
            distanceExpression = leftDistance;
            comparisonOperator = comparisonNode.Operator;
            thresholdLiteral = rightLiteral.Value;
            return true;
        }

        if (comparisonNode.Right is BinaryExpressionNode rightDistance
            && IsVectorDistanceOperator(rightDistance.Operator)
            && comparisonNode.Left is LiteralNode leftLiteral)
        {
            distanceExpression = rightDistance;
            comparisonOperator = InvertRelationalOperator(comparisonNode.Operator);
            thresholdLiteral = leftLiteral.Value;
            return true;
        }

        return false;
    }

    private static bool IsRelationalComparisonOperator(string op)
    {
        return op == Operators.LESS_THAN
            || op == Operators.LESS_THAN_OR_EQUAL_TO
            || op == Operators.GREATER_THAN
            || op == Operators.GREATER_THAN_OR_EQUAL_TO;
    }

    private static bool IsVectorDistanceOperator(string op)
    {
        return op == Operators.VECTOR_DISTANCE_COSINE
            || op == Operators.VECTOR_DISTANCE_L2;
    }

    private static string InvertRelationalOperator(string op)
    {
        return op switch
        {
            Operators.LESS_THAN => Operators.GREATER_THAN,
            Operators.LESS_THAN_OR_EQUAL_TO => Operators.GREATER_THAN_OR_EQUAL_TO,
            Operators.GREATER_THAN => Operators.LESS_THAN,
            Operators.GREATER_THAN_OR_EQUAL_TO => Operators.LESS_THAN_OR_EQUAL_TO,
            _ => op
        };
    }

    /// <summary>
    /// Evaluates a constant comparison expression between two literal values using SQL comparison semantics.
    /// </summary>
    /// <param name="op">The relational operator to apply.</param>
    /// <param name="leftVal">The left literal value.</param>
    /// <param name="rightVal">The right literal value.</param>
    /// <returns><see langword="true"/> if the condition holds; otherwise <see langword="false"/>.</returns>
    /// <exception cref="SecurityException">Thrown when <paramref name="op"/> is not a recognized operator.</exception>
    internal static bool DetermineConstantCondition(string op, object? leftVal, object? rightVal)
    {
        return op switch
        {
            Operators.EQUALS => EvaluateEquality(leftVal, rightVal),
            Operators.NOT_EQUALS => ExpressionValueComparer.SqlNotEquals(leftVal, rightVal, trimQuotedStrings: true),
            Operators.LESS_THAN => CompareDynamics(leftVal, rightVal) < 0,
            Operators.GREATER_THAN => CompareDynamics(leftVal, rightVal) > 0,
            Operators.LESS_THAN_OR_EQUAL_TO => CompareDynamics(leftVal, rightVal) <= 0,
            Operators.GREATER_THAN_OR_EQUAL_TO => CompareDynamics(leftVal, rightVal) >= 0,
            Operators.LIKE => ExpressionValueComparer.MatchesLike(leftVal, rightVal, trimQuotedStrings: true),
            Operators.NOT_LIKE => ExpressionValueComparer.MatchesNotLike(leftVal, rightVal, trimQuotedStrings: true),
            Operators.IS_NULL => leftVal == null,
            Operators.IS_NOT_NULL => leftVal != null,
            _ => throw new SecurityException("Invalid operator")
        };
    }

    /// <summary>
    /// Compares two dynamically typed values for SQL equality, trimming quoted strings and returning
    /// <see langword="false"/> if either operand is <see langword="null"/>.
    /// </summary>
    internal static bool EvaluateEquality(object? leftVal, object? rightVal)
    {
        if (leftVal == null || rightVal == null)
        {
            return false;
        }

        return ExpressionValueComparer.AreEqual(leftVal, rightVal, trimQuotedStrings: true);
    }

    /// <summary>
    /// Performs an ordered comparison between two dynamically typed values, returning <see langword="null"/>
    /// when either operand is <see langword="null"/>.
    /// </summary>
    internal static int? CompareDynamics(object? left, object? right)
    {
        if (left == null || right == null)
        {
            return null;
        }

        return ExpressionValueComparer.Compare(left, right, trimQuotedStrings: true);
    }

    /// <summary>
    /// Converts a vector-distance threshold literal (numeric or quoted/unquoted string) to a <see cref="double"/>.
    /// </summary>
    internal static bool TryConvertThreshold(object? rawValue, out double threshold)
    {
        threshold = 0d;
        if (rawValue is null)
        {
            return false;
        }

        return rawValue switch
        {
            byte v => Assign(v, out threshold),
            sbyte v => Assign(v, out threshold),
            short v => Assign(v, out threshold),
            ushort v => Assign(v, out threshold),
            int v => Assign(v, out threshold),
            uint v => Assign(v, out threshold),
            long v => Assign(v, out threshold),
            ulong v => Assign(v, out threshold),
            float v => Assign(v, out threshold),
            double v => Assign(v, out threshold),
            decimal v => Assign((double)v, out threshold),
            string s => TryParseInvariantDouble(s, out threshold),
            _ => false
        };

        static bool Assign(double value, out double output)
        {
            output = value;
            return true;
        }

        static bool TryParseInvariantDouble(string value, out double output)
        {
            string candidate = value.Trim();
            if ((candidate.StartsWith("'", StringComparison.Ordinal) && candidate.EndsWith("'", StringComparison.Ordinal))
                || (candidate.StartsWith("\"", StringComparison.Ordinal) && candidate.EndsWith("\"", StringComparison.Ordinal)))
            {
                candidate = candidate[1..^1].Trim();
            }

            if (double.TryParse(
                candidate.AsSpan(),
                NumberStyles.Float,
                CultureInfo.InvariantCulture,
                out output))
            {
                return true;
            }

            return double.TryParse(
                candidate.AsSpan(),
                NumberStyles.Float,
                CultureInfo.CurrentCulture,
                out output);
        }
    }

    /// <summary>
    /// Evaluates a relational comparison between a computed vector distance and a numeric threshold.
    /// </summary>
    internal static bool EvaluateDistanceComparison(double distance, double threshold, string comparisonOperator)
    {
        return comparisonOperator switch
        {
            Operators.LESS_THAN => distance < threshold,
            Operators.LESS_THAN_OR_EQUAL_TO => distance <= threshold,
            Operators.GREATER_THAN => distance > threshold,
            Operators.GREATER_THAN_OR_EQUAL_TO => distance >= threshold,
            _ => false
        };
    }
}
