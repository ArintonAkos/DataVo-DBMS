using DataVo.Core.Contracts.Results;
using DataVo.Core.Enums;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.DQL;
using DataVo.Core.Runtime;

namespace DataVo.Core.Parser.Statements.Mechanism;

internal static class SubqueryExpressionMaterializer
{
    public static ExpressionNode Materialize(ExpressionNode node, string databaseName, DataVoEngine engine)
    {
        return node switch
        {
            BinaryExpressionNode binary => new BinaryExpressionNode
            {
                Operator = binary.Operator,
                Left = Materialize(binary.Left, databaseName, engine),
                Right = Materialize(binary.Right, databaseName, engine)
            },
            InSubqueryExpressionNode inSubquery => MaterializeInSubquery(inSubquery, databaseName, engine),
            _ => node
        };
    }

    private static ExpressionNode MaterializeInSubquery(InSubqueryExpressionNode node, string databaseName, DataVoEngine engine)
    {
        QueryResult subqueryResult = ExecuteSubquery(node.Subquery, databaseName, engine);

        if (subqueryResult.IsError)
        {
            throw new Exception(subqueryResult.Messages.FirstOrDefault() ?? "Subquery execution failed.");
        }

        if (subqueryResult.Fields.Count != 1)
        {
            throw new Exception("IN subquery must return exactly one column.");
        }

        string fieldName = subqueryResult.Fields[0];
        ExpressionNode? combined = null;

        foreach (var row in subqueryResult.Data)
        {
            row.TryGetValue(fieldName, out var value);

            ExpressionNode comparison = new BinaryExpressionNode
            {
                Operator = Operators.EQUALS,
                Left = CloneExpression(node.Left),
                Right = ToLiteralNode(value)
            };

            combined = combined == null
                ? comparison
                : new BinaryExpressionNode
                {
                    Operator = Operators.OR,
                    Left = combined,
                    Right = comparison
                };
        }

        return combined ?? new LiteralNode { Value = false };
    }

    private static QueryResult ExecuteSubquery(SqlStatement subquery, string databaseName, DataVoEngine engine)
    {
        Guid session = Guid.NewGuid();
        engine.Sessions.Set(session, databaseName);

        BaseDbAction action = subquery switch
        {
            SelectStatement selectStatement => new Select(selectStatement),
            UnionSelectStatement unionSelectStatement => new UnionSelect(unionSelectStatement),
            _ => throw new Exception("Unsupported subquery statement.")
        };

        action.UseEngine(engine);
        return action.Perform(session);
    }

    private static ExpressionNode ToLiteralNode(object? value)
    {
        return value == null
            ? new NullLiteralNode()
            : new LiteralNode { Value = value };
    }

    private static ExpressionNode CloneExpression(ExpressionNode node)
    {
        return node switch
        {
            BinaryExpressionNode binary => new BinaryExpressionNode
            {
                Operator = binary.Operator,
                Left = CloneExpression(binary.Left),
                Right = CloneExpression(binary.Right)
            },
            ColumnRefNode column => new ColumnRefNode { TableOrAlias = column.TableOrAlias, Column = column.Column },
            ResolvedColumnRefNode resolved => new ResolvedColumnRefNode { TableName = resolved.TableName, Column = resolved.Column },
            NullLiteralNode => new NullLiteralNode(),
            LiteralNode literal => new LiteralNode { Value = literal.Value },
            _ => throw new Exception($"Unsupported expression node '{node.GetType().Name}' in subquery materialization.")
        };
    }
}