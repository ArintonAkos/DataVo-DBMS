using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.BTree;
using DataVo.Core.StorageEngine;
using DataVo.Core.Exceptions;
using DataVo.Core.Parser.Utils;
using System.Security;

namespace DataVo.Core.Parser.Statements.Mechanism
{
    internal class StatementEvaluatorWOJoin
    {
        private readonly TableDetail _table;

        public StatementEvaluatorWOJoin(string databaseName, string tableName)
        {
            _table = new TableDetail(tableName, null);
            _table.DatabaseName = databaseName;
        }

        public HashSet<string> Evaluate(ExpressionNode root)
        {
            if (root is LiteralNode literalNode)
            {
                if (literalNode.Value is string s && s == "1=1")
                {
                    return _table.TableContent!.Select(row => row.Key).ToHashSet();
                }
                if (literalNode.Value is bool b && b)
                {
                    return _table.TableContent!.Select(row => row.Key).ToHashSet();
                }

                return new();
            }

            if (root is not BinaryExpressionNode binNode)
            {
                throw new EvaluationException("Invalid expression tree node type: expected BinaryExpressionNode or LiteralNode.");
            }

            bool isLogical = binNode.Operator == "AND" || binNode.Operator == "OR";

            if (!isLogical)
            {
                var comparisonNode = NormalizeComparisonNode(binNode);

                if (comparisonNode.Left is ResolvedColumnRefNode && comparisonNode.Right is ResolvedColumnRefNode)
                {
                    return HandleTwoColumnExpression(comparisonNode);
                }

                if (comparisonNode.Operator == "=")
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

            var leftResult = Evaluate(binNode.Left);
            var rightResult = Evaluate(binNode.Right);

            if (binNode.Operator == "AND")
            {
                return new HashSet<string>(leftResult.Intersect(rightResult));
            }

            if (binNode.Operator == "OR")
            {
                return new HashSet<string>(leftResult.Union(rightResult));
            }

            throw new EvaluationException($"Invalid expression operator: {binNode.Operator}");
        }

        private static BinaryExpressionNode NormalizeComparisonNode(BinaryExpressionNode node)
        {
            if (node.Left is LiteralNode && node.Right is ResolvedColumnRefNode)
            {
                string normalizedOperator = node.Operator switch
                {
                    "<" => ">",
                    ">" => "<",
                    "<=" => ">=",
                    ">=" => "<=",
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

        private HashSet<string> HandleIndexableStatement(BinaryExpressionNode root)
        {
            var leftCol = (ResolvedColumnRefNode)root.Left;
            var rightLit = (LiteralNode)root.Right;

            string leftValue = leftCol.Column;
            string rightValue = rightLit.Value?.ToString() ?? string.Empty;

            _table.IndexedColumns!.TryGetValue(leftValue, out string? indexFile);
            if (indexFile != null)
            {
                return IndexManager.Instance.FilterUsingIndex(rightValue, indexFile, _table.TableName, _table.DatabaseName!);
            }

            int columnIndex = _table.PrimaryKeys!.IndexOf(leftValue);
            if (columnIndex > -1)
            {
                return IndexManager.Instance.FilterUsingIndex(rightValue, $"_PK_{_table.TableName}", _table.TableName, _table.DatabaseName!);
            }

            return _table.TableContent!
                .Where(entry =>
                {
                    return EvaluateEquality(entry.Value[leftValue], rightLit.Value);
                })
                .Select(entry => entry.Key)
                .ToHashSet();
        }

        private bool EvaluateEquality(dynamic? leftVal, dynamic? rightVal)
        {
            return ExpressionValueComparer.AreEqual(leftVal, rightVal);
        }

        private HashSet<string> HandleNonIndexableStatement(BinaryExpressionNode root)
        {
            var leftCol = (ResolvedColumnRefNode)root.Left;
            var rightLit = (LiteralNode)root.Right;

            string leftValue = leftCol.Column;
            var rightVal = rightLit.Value;

            Func<KeyValuePair<string, Dictionary<string, dynamic>>, bool> pred = root.Operator switch
            {
                "=" => entry => Convert.ToString(entry.Value[leftValue]) == Convert.ToString(rightVal),
                "!=" => entry => Convert.ToString(entry.Value[leftValue]) != Convert.ToString(rightVal),
                "<" => entry => CompareDynamics(entry.Value[leftValue], rightVal) < 0,
                ">" => entry => CompareDynamics(entry.Value[leftValue], rightVal) > 0,
                "<=" => entry => CompareDynamics(entry.Value[leftValue], rightVal) <= 0,
                ">=" => entry => CompareDynamics(entry.Value[leftValue], rightVal) >= 0,
                _ => throw new SecurityException("Invalid operator")
            };

            return _table.TableContent!
                .Where(pred)
                .Select(entry => entry.Key)
                .ToHashSet();
        }

        private HashSet<string> HandleTwoColumnExpression(BinaryExpressionNode root)
        {
            var leftCol = (ResolvedColumnRefNode)root.Left;
            var rightCol = (ResolvedColumnRefNode)root.Right;

            string leftValue = leftCol.Column;
            string rightValue = rightCol.Column;

            Func<KeyValuePair<string, Dictionary<string, dynamic>>, bool> pred = root.Operator switch
            {
                "=" => entry => Convert.ToString(entry.Value[leftValue]) == Convert.ToString(entry.Value[rightValue]),
                "!=" => entry => Convert.ToString(entry.Value[leftValue]) != Convert.ToString(entry.Value[rightValue]),
                "<" => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) < 0,
                ">" => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) > 0,
                "<=" => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) <= 0,
                ">=" => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) >= 0,
                _ => throw new SecurityException("Invalid operator")
            };

            return _table.TableContent!
                .Where(pred)
                .Select(entry => entry.Key)
                .ToHashSet();
        }

        private HashSet<string> HandleConstantExpression(BinaryExpressionNode root)
        {
            var leftLit = (LiteralNode)root.Left;
            var rightLit = (LiteralNode)root.Right;

            object? leftVal = leftLit.Value;
            object? rightVal = rightLit.Value;

            bool isCondTrue = root.Operator switch
            {
                "=" => Convert.ToString(leftVal) == Convert.ToString(rightVal),
                "!=" => Convert.ToString(leftVal) != Convert.ToString(rightVal),
                "<" => CompareDynamics(leftVal, rightVal) < 0,
                ">" => CompareDynamics(leftVal, rightVal) > 0,
                "<=" => CompareDynamics(leftVal, rightVal) <= 0,
                ">=" => CompareDynamics(leftVal, rightVal) >= 0,
                _ => throw new SecurityException("Invalid operator")
            };

            return isCondTrue
                ? _table.TableContent!.Select(row => row.Key).ToHashSet()
                : new();
        }

        private int CompareDynamics(dynamic? left, dynamic? right)
        {
            return ExpressionValueComparer.Compare(left, right);
        }
    }
}
