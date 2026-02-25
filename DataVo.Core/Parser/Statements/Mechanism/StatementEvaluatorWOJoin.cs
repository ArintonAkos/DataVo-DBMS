using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.BTree;
using DataVo.Core.StorageEngine;
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
                throw new Exception("Invalid tree node type: expected BinaryExpressionNode or LiteralNode for condition.");
            }

            bool isLogical = binNode.Operator == "AND" || binNode.Operator == "OR";

            if (!isLogical)
            {
                if (binNode.Left is ResolvedColumnRefNode && binNode.Right is ResolvedColumnRefNode)
                {
                    return HandleTwoColumnExpression(binNode);
                }

                if (binNode.Left is LiteralNode && binNode.Right is ResolvedColumnRefNode)
                {
                    (binNode.Right, binNode.Left) = (binNode.Left, binNode.Right);

                    switch (binNode.Operator)
                    {
                        case "<": binNode.Operator = ">"; break;
                        case ">": binNode.Operator = "<"; break;
                        case "<=": binNode.Operator = ">="; break;
                        case ">=": binNode.Operator = "<="; break;
                        default: break;
                    }
                }

                if (binNode.Operator == "=")
                {
                    if (binNode.Left is ResolvedColumnRefNode && binNode.Right is LiteralNode)
                    {
                        return HandleIndexableStatement(binNode);
                    }

                    if (binNode.Left is LiteralNode && binNode.Right is LiteralNode)
                    {
                        return HandleConstantExpression(binNode);
                    }
                }

                if (binNode.Left is ResolvedColumnRefNode && binNode.Right is LiteralNode)
                {
                    return HandleNonIndexableStatement(binNode);
                }

                if (binNode.Left is LiteralNode && binNode.Right is LiteralNode)
                {
                    return HandleConstantExpression(binNode);
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

            throw new Exception($"Invalid tree node operator: {binNode.Operator}");
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
            if (leftVal == null && rightVal == null) return true;
            if (leftVal == null || rightVal == null) return false;

            if (leftVal is IConvertible lConv && rightVal is IConvertible rConv)
            {
                try
                {
                    // Attempt string equality first, if not equal, try numeric
                    if (Convert.ToString(leftVal) == Convert.ToString(rightVal)) return true;

                    double lNum = lConv.ToDouble(null);
                    double rNum = rConv.ToDouble(null);
                    return lNum == rNum;
                }
                catch (Exception)
                {
                }
            }

            return Convert.ToString(leftVal) == Convert.ToString(rightVal);
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
            if (left == null && right == null) return 0;
            if (left == null) return -1;
            if (right == null) return 1;

            if (left is IConvertible lConv && right is IConvertible rConv)
            {
                try
                {
                    double lNum = lConv.ToDouble(null);
                    double rNum = rConv.ToDouble(null);
                    return lNum.CompareTo(rNum);
                }
                catch (Exception)
                {
                }
            }

            try
            {
                return Comparer<dynamic>.Default.Compare(left, right);
            }
            catch
            {
                return string.Compare(Convert.ToString(left), Convert.ToString(right), StringComparison.Ordinal);
            }
        }
    }
}
