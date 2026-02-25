using DataVo.Core.Enums;
using DataVo.Core.Exceptions;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Utils;

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
            return s == "1=1";
        }

        return false;
    }

    protected abstract TResult EvaluateTrueLiteral();
    protected abstract TResult EvaluateFalseLiteral();
    protected abstract TResult HandleIndexableStatement(BinaryExpressionNode root);
    protected abstract TResult HandleNonIndexableStatement(BinaryExpressionNode root);
    protected abstract TResult HandleTwoColumnExpression(BinaryExpressionNode root);
    protected abstract TResult HandleConstantExpression(BinaryExpressionNode root);
    protected abstract TResult And(TResult leftResult, TResult rightResult);
    protected abstract TResult Or(TResult leftResult, TResult rightResult);
}
