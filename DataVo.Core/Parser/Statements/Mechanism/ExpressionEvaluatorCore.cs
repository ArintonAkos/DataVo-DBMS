using DataVo.Core.Enums;
using DataVo.Core.Exceptions;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Utils;
using DataVo.Core.Constants;

namespace DataVo.Core.Parser.Statements.Mechanism;

public abstract class ExpressionEvaluatorCore<TResult>
{
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

    protected abstract TResult EvaluateTrueLiteral();
    protected abstract TResult EvaluateFalseLiteral();
    protected abstract TResult HandleIndexableStatement(BinaryExpressionNode root);
    protected abstract TResult HandleNonIndexableStatement(BinaryExpressionNode root);
    protected abstract TResult HandleDistanceThresholdExpression(BinaryExpressionNode distanceExpression, string comparisonOperator, object? thresholdLiteral);
    protected abstract TResult HandleTwoColumnExpression(BinaryExpressionNode root);
    protected abstract TResult HandleConstantExpression(BinaryExpressionNode root);
    protected abstract TResult And(TResult leftResult, TResult rightResult);
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
