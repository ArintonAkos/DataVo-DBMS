using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Types;
using DataVo.Core.BTree;
using DataVo.Core.StorageEngine;
using DataVo.Core.Services;
using DataVo.Core.Utils;
using System.Security;

namespace DataVo.Core.Parser.Statements;

public class StatementEvaluator
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

    public HashedTable Evaluate(ExpressionNode root)
    {
        if (root is LiteralNode literalNode)
        {
            if (literalNode.Value is string s && s == "1=1")
            {
                return GetJoinedTableContent(FromTable!.TableContent!, FromTable.TableName);
            }
            if (literalNode.Value is bool b && b)
            {
                return GetJoinedTableContent(FromTable!.TableContent!, FromTable.TableName);
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
            // Operator is = != < > <= >=
            if (binNode.Left is ResolvedColumnRefNode && binNode.Right is ResolvedColumnRefNode)
            {
                return HandleTwoColumnExpression(binNode);
            }

            if (binNode.Left is LiteralNode && binNode.Right is ResolvedColumnRefNode)
            {
                // Swap left and right
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

            // Other operators
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
            return And(leftResult, rightResult);
        }

        if (binNode.Operator == "OR")
        {
            return Or(leftResult, rightResult);
        }

        throw new Exception($"Invalid tree node operator: {binNode.Operator}");
    }

    private HashedTable HandleIndexableStatement(BinaryExpressionNode root)
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

    private HashedTable HandleNonIndexableStatement(BinaryExpressionNode root)
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
            "=" => entry => EvaluateEquality(entry.Value[leftValue], rightVal),
            "!=" => entry => !EvaluateEquality(entry.Value[leftValue], rightVal),
            "<" => entry => CompareDynamics(entry.Value[leftValue], rightVal) < 0,
            ">" => entry => CompareDynamics(entry.Value[leftValue], rightVal) > 0,
            "<=" => entry => CompareDynamics(entry.Value[leftValue], rightVal) <= 0,
            ">=" => entry => CompareDynamics(entry.Value[leftValue], rightVal) >= 0,
            _ => throw new SecurityException("Invalid operator")
        };

        Dictionary<string, Dictionary<string, dynamic>> tableRows = table.TableContent!
            .Where(pred)
            .ToDictionary(t => t.Key, t => t.Value);

        return GetJoinedTableContent(tableRows, table.TableName);
    }

    private HashedTable HandleTwoColumnExpression(BinaryExpressionNode root)
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
            "=" => entry => EvaluateEquality(entry.Value[leftValue], entry.Value[rightValue]),
            "!=" => entry => !EvaluateEquality(entry.Value[leftValue], entry.Value[rightValue]),
            "<" => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) < 0,
            ">" => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) > 0,
            "<=" => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) <= 0,
            ">=" => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) >= 0,
            _ => throw new SecurityException("Invalid operator")
        };

        Dictionary<string, Dictionary<string, dynamic>> tableRows = table.TableContent!
            .Where(pred)
            .ToDictionary(t => t.Key, t => t.Value);

        return GetJoinedTableContent(tableRows, table.TableName);
    }

    private HashedTable HandleConstantExpression(BinaryExpressionNode root)
    {
        var leftLit = (LiteralNode)root.Left;
        var rightLit = (LiteralNode)root.Right;

        object? leftVal = leftLit.Value;
        object? rightVal = rightLit.Value;

        bool isCondTrue = root.Operator switch
        {
            "=" => EvaluateEquality(leftVal, rightVal),
            "!=" => !EvaluateEquality(leftVal, rightVal),
            "<" => CompareDynamics(leftVal, rightVal) < 0,
            ">" => CompareDynamics(leftVal, rightVal) > 0,
            "<=" => CompareDynamics(leftVal, rightVal) <= 0,
            ">=" => CompareDynamics(leftVal, rightVal) >= 0,
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

    private static HashedTable And(HashedTable leftResult, HashedTable rightResult)
    {
        var result = leftResult.Keys.Intersect(rightResult.Keys)
               .ToDictionary(t => t, t => leftResult[t]);

        return new HashedTable(result);
    }

    private static HashedTable Or(HashedTable leftResult, HashedTable rightResult)
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
        if (leftVal == null && rightVal == null) return true;
        if (leftVal == null || rightVal == null) return false;

        if (leftVal is IConvertible lConv && rightVal is IConvertible rConv)
        {
            try
            {
                if (Convert.ToString(leftVal) == Convert.ToString(rightVal)) return true;
                
                double lNum = lConv.ToDouble(null);
                double rNum = rConv.ToDouble(null);
                return lNum == rNum;
            }
            catch (Exception)
            {
                // Fallback
            }
        }

        return Convert.ToString(leftVal) == Convert.ToString(rightVal);
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
                // Fallback handled below
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