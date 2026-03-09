using DataVo.Core.Contracts.Results;
using DataVo.Core.Enums;
using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.DQL;
using DataVo.Core.Runtime;
using DataVo.Core.Services;

namespace DataVo.Core.Parser.Statements.Mechanism;

internal static class SubqueryExpressionMaterializer
{
    public static ExpressionNode Materialize(ExpressionNode node, string databaseName, DataVoEngine engine, TableService? outerScope = null)
    {
        return node switch
        {
            BinaryExpressionNode binary => new BinaryExpressionNode
            {
                Operator = binary.Operator,
                Left = Materialize(binary.Left, databaseName, engine, outerScope),
                Right = Materialize(binary.Right, databaseName, engine, outerScope)
            },
            InSubqueryExpressionNode inSubquery => MaterializeInSubquery(inSubquery, databaseName, engine, outerScope),
            _ => node
        };
    }

    private static ExpressionNode MaterializeInSubquery(InSubqueryExpressionNode node, string databaseName, DataVoEngine engine, TableService? outerScope)
    {
        RejectCorrelatedSubquery(node.Subquery, databaseName, outerScope);

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

    private static void RejectCorrelatedSubquery(SqlStatement subquery, string databaseName, TableService? outerScope)
    {
        if (outerScope == null)
        {
            return;
        }

        if (ContainsCorrelatedReference(subquery, databaseName, outerScope))
        {
            throw new Exception("Correlated subqueries are not supported yet.");
        }
    }

    private static bool ContainsCorrelatedReference(SqlStatement subquery, string databaseName, TableService outerScope)
    {
        return subquery switch
        {
            SelectStatement select => ContainsCorrelatedReference(select, databaseName, outerScope),
            UnionSelectStatement union => ContainsCorrelatedReference(union.Left, databaseName, outerScope)
                || union.Branches.Any(branch => ContainsCorrelatedReference(branch.Select, databaseName, outerScope)),
            _ => false
        };
    }

    private static bool ContainsCorrelatedReference(SelectStatement subquery, string databaseName, TableService outerScope)
    {
        var innerScope = BuildInnerScope(subquery, databaseName);

        if (subquery.Columns.Any(column => IsCorrelatedColumnName(column.Expression, innerScope, outerScope)))
        {
            return true;
        }

        if (subquery.GroupByExpression?.Columns.Any(column => IsCorrelatedColumnName(column.Name, innerScope, outerScope)) == true)
        {
            return true;
        }

        if (subquery.OrderByExpression?.Columns.Any(column => IsCorrelatedColumnName(column.Column.Name, innerScope, outerScope)) == true)
        {
            return true;
        }

        return ContainsCorrelatedReference(subquery.WhereExpression, innerScope, outerScope)
            || ContainsCorrelatedReference(subquery.HavingExpression, innerScope, outerScope);
    }

    private static TableService BuildInnerScope(SelectStatement subquery, string databaseName)
    {
        var tableService = new TableService(databaseName);

        if (subquery.FromTable != null)
        {
            tableService.AddTableDetail(new TableDetail(subquery.FromTable.Name, subquery.FromAlias?.Name));
        }

        foreach (var join in subquery.Joins)
        {
            tableService.AddTableDetail(new TableDetail(join.TableName.Name, join.Alias?.Name));
        }

        return tableService;
    }

    private static bool ContainsCorrelatedReference(ExpressionNode? node, TableService innerScope, TableService outerScope)
    {
        if (node == null)
        {
            return false;
        }

        return node switch
        {
            BinaryExpressionNode binary => ContainsCorrelatedReference(binary.Left, innerScope, outerScope)
                || ContainsCorrelatedReference(binary.Right, innerScope, outerScope),
            InSubqueryExpressionNode inSubquery => ContainsCorrelatedReference(inSubquery.Left, innerScope, outerScope)
                || ContainsCorrelatedReference(inSubquery.Subquery, innerScope.DatabaseName, innerScope),
            ColumnRefNode columnRef => IsCorrelatedReference(columnRef, innerScope, outerScope),
            _ => false
        };
    }

    private static bool IsCorrelatedColumnName(string rawExpression, TableService innerScope, TableService outerScope)
    {
        string trimmed = rawExpression.Trim();

        if (trimmed == "*" || trimmed.EndsWith(".*", StringComparison.Ordinal) || trimmed.Contains('('))
        {
            return false;
        }

        ColumnRefNode columnRef = ParseColumnRef(trimmed);
        return IsCorrelatedReference(columnRef, innerScope, outerScope);
    }

    private static ColumnRefNode ParseColumnRef(string rawExpression)
    {
        string[] parts = rawExpression.Split('.', 2);
        return parts.Length == 2
            ? new ColumnRefNode { TableOrAlias = parts[0], Column = parts[1] }
            : new ColumnRefNode { Column = rawExpression };
    }

    private static bool IsCorrelatedReference(ColumnRefNode columnRef, TableService innerScope, TableService outerScope)
    {
        if (CanResolve(columnRef, innerScope))
        {
            return false;
        }

        return CanResolve(columnRef, outerScope);
    }

    private static bool CanResolve(ColumnRefNode columnRef, TableService tableService)
    {
        try
        {
            if (!string.IsNullOrWhiteSpace(columnRef.TableOrAlias))
            {
                TableDetail table = tableService.GetTableDetailByAliasOrName(columnRef.TableOrAlias);
                return table.Columns?.Contains(columnRef.Column) == true;
            }

            tableService.ParseAndFindTableNameByColumn(columnRef.Column);
            return true;
        }
        catch
        {
            return false;
        }
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