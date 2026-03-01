using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.Enums;
using DataVo.Core.BTree;
using DataVo.Core.StorageEngine;
using DataVo.Core.Parser.Utils;
using System.Security;

namespace DataVo.Core.Parser.Statements.Mechanism
{
    internal class StatementEvaluatorWOJoin : ExpressionEvaluatorCore<HashSet<long>>
    {
        private readonly TableDetail _table;

        public StatementEvaluatorWOJoin(string databaseName, string tableName)
        {
            _table = new TableDetail(tableName, null)
            {
                DatabaseName = databaseName
            };
        }

        protected override HashSet<long> EvaluateTrueLiteral()
        {
            return _table.TableContent!.Select(row => row.Key).ToHashSet();
        }

        protected override HashSet<long> EvaluateFalseLiteral() => [];

        protected override HashSet<long> HandleIndexableStatement(BinaryExpressionNode root)
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

            return [.. _table.TableContent!
                .Where(entry =>
                {
                    return EvaluateEquality(entry.Value[leftValue], rightLit.Value);
                })
                .Select(entry => entry.Key)];
        }

        private static bool EvaluateEquality(dynamic? leftVal, dynamic? rightVal)
        {
            return ExpressionValueComparer.AreEqual(leftVal, rightVal);
        }

        protected override HashSet<long> HandleNonIndexableStatement(BinaryExpressionNode root)
        {
            var leftCol = (ResolvedColumnRefNode)root.Left;
            var rightLit = (LiteralNode)root.Right;

            string leftValue = leftCol.Column;
            var rightVal = rightLit.Value;

            Func<KeyValuePair<long, Record>, bool> pred = root.Operator switch
            {
                Operators.EQUALS => entry => EvaluateEquality(entry.Value[leftValue], rightVal),
                Operators.NOT_EQUALS => entry => !EvaluateEquality(entry.Value[leftValue], rightVal),
                Operators.LESS_THAN => entry => CompareDynamics(entry.Value[leftValue], rightVal) < 0,
                Operators.GREATER_THAN => entry => CompareDynamics(entry.Value[leftValue], rightVal) > 0,
                Operators.LESS_THAN_OR_EQUAL_TO => entry => CompareDynamics(entry.Value[leftValue], rightVal) <= 0,
                Operators.GREATER_THAN_OR_EQUAL_TO => entry => CompareDynamics(entry.Value[leftValue], rightVal) >= 0,
                _ => throw new SecurityException("Invalid operator")
            };

            return _table.TableContent!
                .Where(pred)
                .Select(entry => entry.Key)
                .ToHashSet();
        }

        protected override HashSet<long> HandleTwoColumnExpression(BinaryExpressionNode root)
        {
            var leftCol = (ResolvedColumnRefNode)root.Left;
            var rightCol = (ResolvedColumnRefNode)root.Right;

            string leftValue = leftCol.Column;
            string rightValue = rightCol.Column;

            Func<KeyValuePair<long, Record>, bool> pred = root.Operator switch
            {
                Operators.EQUALS => entry => EvaluateEquality(entry.Value[leftValue], entry.Value[rightValue]),
                Operators.NOT_EQUALS => entry => !EvaluateEquality(entry.Value[leftValue], entry.Value[rightValue]),
                Operators.LESS_THAN => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) < 0,
                Operators.GREATER_THAN => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) > 0,
                Operators.LESS_THAN_OR_EQUAL_TO => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) <= 0,
                Operators.GREATER_THAN_OR_EQUAL_TO => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) >= 0,
                _ => throw new SecurityException("Invalid operator")
            };

            return _table.TableContent!
                .Where(pred)
                .Select(entry => entry.Key)
                .ToHashSet();
        }

        protected override HashSet<long> HandleConstantExpression(BinaryExpressionNode root)
        {
            var leftLit = (LiteralNode)root.Left;
            var rightLit = (LiteralNode)root.Right;

            object? leftVal = leftLit.Value;
            object? rightVal = rightLit.Value;

            bool isCondTrue = root.Operator switch
            {
                Operators.EQUALS => EvaluateEquality(leftVal, rightVal),
                Operators.NOT_EQUALS => !EvaluateEquality(leftVal, rightVal),
                Operators.LESS_THAN => CompareDynamics(leftVal, rightVal) < 0,
                Operators.GREATER_THAN => CompareDynamics(leftVal, rightVal) > 0,
                Operators.LESS_THAN_OR_EQUAL_TO => CompareDynamics(leftVal, rightVal) <= 0,
                Operators.GREATER_THAN_OR_EQUAL_TO => CompareDynamics(leftVal, rightVal) >= 0,
                _ => throw new SecurityException("Invalid operator")
            };

            return isCondTrue
                ? _table.TableContent!.Select(row => row.Key).ToHashSet()
                : new();
        }

        protected override HashSet<long> And(HashSet<long> leftResult, HashSet<long> rightResult)
        {
            return [.. leftResult.Intersect(rightResult)];
        }

        protected override HashSet<long> Or(HashSet<long> leftResult, HashSet<long> rightResult)
        {
            return [.. leftResult.Union(rightResult)];
        }

        private static int CompareDynamics(dynamic? left, dynamic? right)
        {
            return ExpressionValueComparer.Compare(left, right);
        }
    }
}
