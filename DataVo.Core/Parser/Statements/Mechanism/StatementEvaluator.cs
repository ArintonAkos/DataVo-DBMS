using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.Enums;
using DataVo.Core.Parser.Types;
using DataVo.Core.BTree;
using DataVo.Core.StorageEngine;
using DataVo.Core.Services;
using DataVo.Core.Utils;
using DataVo.Core.Parser.Utils;
using DataVo.Core.Parser.Statements.Mechanism;
using System.Security;

namespace DataVo.Core.Parser.Statements;

public class StatementEvaluator : ExpressionEvaluatorCore<HashedTable>
{
    private TableService TableService { get; set; }
    private Join? Join { get; set; }
    private TableDetail? FromTable { get; set; }

    public StatementEvaluator(TableService tableService, Join joinStatements, TableDetail fromTable)
    {
        TableService = tableService;
        Join = joinStatements;
        FromTable = fromTable;
    }

    protected override HashedTable EvaluateTrueLiteral()
    {
        return GetJoinedTableContent(FromTable!.TableContent!, FromTable.TableName);
    }

    protected override HashedTable EvaluateFalseLiteral() => new();

    protected override HashedTable HandleIndexableStatement(BinaryExpressionNode root)
    {
        var leftCol = (ResolvedColumnRefNode)root.Left;
        var rightLit = (LiteralNode)root.Right;

        string tableName = leftCol.TableName;
        string leftValue = leftCol.Column;
        string rightValue = rightLit.Value?.ToString() ?? string.Empty;

        var table = TableService.TableDetails[tableName];

        Dictionary<string, Dictionary<string, dynamic>> tableRows;

        table.IndexedColumns!.TryGetValue(leftValue, out string? indexFile);
        if (indexFile != null)
        {
            List<string> ids = IndexManager.Instance.FilterUsingIndex(rightValue, indexFile, table.TableName, table.DatabaseName!).ToList();
            List<long> longIds = ids.Select(id => long.Parse(id)).ToList();

            var internalRows = StorageContext.Instance.SelectFromTable(longIds, new(), table.TableName, table.DatabaseName!);

            tableRows = new Dictionary<string, Dictionary<string, dynamic>>();
            foreach (var kvp in internalRows)
            {
                tableRows[kvp.Key.ToString()] = kvp.Value;
            }

            return GetJoinedTableContent(tableRows, table.TableName);
        }

        int columnIndex = table.PrimaryKeys!.IndexOf(leftValue);
        if (columnIndex > -1)
        {
            List<string> ids = IndexManager.Instance.FilterUsingIndex(rightValue, $"_PK_{table.TableName}", table.TableName, table.DatabaseName!).ToList();
            List<long> longIds = ids.Select(id => long.Parse(id)).ToList();

            var internalRows = StorageContext.Instance.SelectFromTable(longIds, new(), table.TableName, table.DatabaseName!);

            tableRows = new Dictionary<string, Dictionary<string, dynamic>>();
            foreach (var kvp in internalRows)
            {
                tableRows[kvp.Key.ToString()] = kvp.Value;
            }

            return GetJoinedTableContent(tableRows, table.TableName);
        }

        tableRows = table.TableContent!
            .Where(entry => entry.Value[leftValue].ToString() == rightValue) // Compare string reps just in case
            .ToDictionary(t => t.Key, t => t.Value);

        return GetJoinedTableContent(tableRows, table.TableName);
    }

    protected override HashedTable HandleNonIndexableStatement(BinaryExpressionNode root)
    {
        var leftCol = (ResolvedColumnRefNode)root.Left;
        var rightLit = (LiteralNode)root.Right;

        string tableName = leftCol.TableName;
        string leftValue = leftCol.Column;
        var rightVal = rightLit.Value;

        var table = TableService.TableDetails[tableName];

        // Ensure dynamic comparisons can be made
        Func<KeyValuePair<string, Dictionary<string, dynamic>>, bool> pred = root.Operator switch
        {
            Operators.EQUALS => entry => EvaluateEquality(entry.Value[leftValue], rightVal),
            Operators.NOT_EQUALS => entry => !EvaluateEquality(entry.Value[leftValue], rightVal),
            Operators.LESS_THAN => entry => CompareDynamics(entry.Value[leftValue], rightVal) < 0,
            Operators.GREATER_THAN => entry => CompareDynamics(entry.Value[leftValue], rightVal) > 0,
            Operators.LESS_THAN_OR_EQUAL_TO => entry => CompareDynamics(entry.Value[leftValue], rightVal) <= 0,
            Operators.GREATER_THAN_OR_EQUAL_TO => entry => CompareDynamics(entry.Value[leftValue], rightVal) >= 0,
            _ => throw new SecurityException("Invalid operator")
        };

        Dictionary<string, Dictionary<string, dynamic>> tableRows = table.TableContent!
            .Where(pred)
            .ToDictionary(t => t.Key, t => t.Value);

        return GetJoinedTableContent(tableRows, table.TableName);
    }

    protected override HashedTable HandleTwoColumnExpression(BinaryExpressionNode root)
    {
        var leftCol = (ResolvedColumnRefNode)root.Left;
        var rightCol = (ResolvedColumnRefNode)root.Right;

        string tableName = leftCol.TableName;
        string rightTableName = rightCol.TableName;

        if (tableName != rightTableName)
        {
            throw new SecurityException("Join like statement not permitted in where clause!");
        }

        string leftValue = leftCol.Column;
        string rightValue = rightCol.Column;

        var table = TableService.TableDetails[tableName];

        Func<KeyValuePair<string, Dictionary<string, dynamic>>, bool> pred = root.Operator switch
        {
            Operators.EQUALS => entry => EvaluateEquality(entry.Value[leftValue], entry.Value[rightValue]),
            Operators.NOT_EQUALS => entry => !EvaluateEquality(entry.Value[leftValue], entry.Value[rightValue]),
            Operators.LESS_THAN => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) < 0,
            Operators.GREATER_THAN => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) > 0,
            Operators.LESS_THAN_OR_EQUAL_TO => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) <= 0,
            Operators.GREATER_THAN_OR_EQUAL_TO => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) >= 0,
            _ => throw new SecurityException("Invalid operator")
        };

        Dictionary<string, Dictionary<string, dynamic>> tableRows = table.TableContent!
            .Where(pred)
            .ToDictionary(t => t.Key, t => t.Value);

        return GetJoinedTableContent(tableRows, table.TableName);
    }

    protected override HashedTable HandleConstantExpression(BinaryExpressionNode root)
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

        if (isCondTrue)
        {
            return GetJoinedTableContent(FromTable!.TableContent!, FromTable.TableName);
        }

        return new();
    }

    private HashedTable GetJoinedTableContent(Dictionary<string, Dictionary<string, dynamic>> tableRows, string tableName)
    {
        HashedTable groupedInitialTable = new();

        foreach (var row in tableRows)
        {
            groupedInitialTable.Add(row.Key, new JoinedRow(tableName, row.Value.ToRow()));
        }

        return Join!.Evaluate(groupedInitialTable);
    }

    protected override HashedTable And(HashedTable leftResult, HashedTable rightResult)
    {
        var result = leftResult.Keys.Intersect(rightResult.Keys)
               .ToDictionary(t => t, t => leftResult[t]);

        return new HashedTable(result);
    }

    protected override HashedTable Or(HashedTable leftResult, HashedTable rightResult)
    {
        HashSet<string> leftHashes = [.. leftResult.Keys];
        HashSet<string> rightHashes = [.. rightResult.Keys];

        HashSet<string> unionResult = [.. leftHashes.Union(rightHashes)];

        HashedTable result = [];
        foreach (string hash in unionResult)
        {
            if (leftResult.ContainsKey(hash))
            {
                result.Add(hash, leftResult[hash]);
                continue;
            }

            result.Add(hash, rightResult[hash]);
        }

        return result;
    }

    private bool EvaluateEquality(dynamic? leftVal, dynamic? rightVal)
    {
        return ExpressionValueComparer.AreEqual(leftVal, rightVal);
    }

    private int CompareDynamics(dynamic? left, dynamic? right)
    {
        return ExpressionValueComparer.Compare(left, right);
    }
}