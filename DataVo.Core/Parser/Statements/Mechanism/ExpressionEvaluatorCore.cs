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
}
