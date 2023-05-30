using Server.Models.Catalog;
using Server.Models.Statement.Utils;
using Server.Server.MongoDB;
using Server.Services;
using System.Linq;
using System.Net.WebSockets;
using System.Security;
using static Server.Models.Statement.JoinModel;

namespace Server.Parser.Statements;
using TableRows = List<Dictionary<string, Dictionary<string, dynamic>>>;

internal class StatementEvaluator
{

    private TableService tableService { get; set; }
    private Join? join { get; set; }
    private TableDetail? fromTable { get; set; } 

    public StatementEvaluator(string databaseName, string tableName, TableDetail fromTable)
    {
        tableService = new TableService(databaseName);
        tableService.AddTableDetail(new TableDetail(tableName, null));
        this.fromTable = fromTable;
    }

    public StatementEvaluator(TableService tableService, Join joinStatements, TableDetail fromTable)
    {
        this.tableService = tableService;
        this.join = joinStatements;
        this.fromTable = fromTable;
    }

    public TableRows Evaluate(Node root)
    {
        if ((root.Type == Node.NodeType.Eq || root.Type == Node.NodeType.Operator)
            && root.Left!.Type == Node.NodeType.Column && root.Right!.Type == Node.NodeType.Column)
        {
            TableDetail leftTable = tableService.GetTableDetailByColumn(root.Left.Value.ParsedValue);
            TableDetail rightTable = tableService.GetTableDetailByColumn(root.Right.Value.ParsedValue);

            if (leftTable.TableName != rightTable.TableName)
            {
                throw new Exception("Join like statement not permitted in where clause!");
            }

            // List<Dictionary<string, Dictionary<string, dynamic>>> result
            List<string> ids = HandleTwoColumnExpression(root, leftTable).ToList();
            var tableRows = DbContext.Instance.SelectFromTable(ids, new(), leftTable.TableName, leftTable.DatabaseName!)
                .Select(row => row.Value)
                .ToList();

            return GetJoinedTableContent(tableRows, leftTable.TableName, leftTable.DatabaseName!);
        }

        if ((root.Type == Node.NodeType.Operator || root.Type == Node.NodeType.Eq)
            && root.Left!.Type == Node.NodeType.Value
            && root.Right!.Type == Node.NodeType.Column)
        {
            (root.Right, root.Left) = (root.Left, root.Right);

            switch (root.Value.ParsedValue)
            {
                case "<": root.Value.Value = ">"; break;
                case ">": root.Value.Value = "<"; break;
                case "<=": root.Value.Value = ">="; break;
                case ">=": root.Value.Value = "<="; break;
                default: break;
            }
        }

        if (root.Type == Node.NodeType.Eq)
        {
            if (root.Left!.Type == Node.NodeType.Column && root.Right!.Type == Node.NodeType.Value)
            {
                TableDetail table = tableService.GetTableDetailByColumn(root.Left.Value.ParsedValue);
                List<string> ids = HandleNonIndexableStatement(root, table).ToList();
                var tableRows = DbContext.Instance.SelectFromTable(ids, new(), table.TableName, table.DatabaseName!)
                    .Select(row => row.Value)
                    .ToList();

                return GetJoinedTableContent(tableRows, table.TableName, table.DatabaseName!);
            }

            return HandleConstantExpressionWithJoin(root);
        }

        if (root.Type == Node.NodeType.Operator)
        {
            if (root.Left!.Type == Node.NodeType.Column)
            {
                TableDetail table = tableService.GetTableDetailByColumn(root.Left.Value.ParsedValue);
                List<string> ids = HandleNonIndexableStatement(root, table).ToList();
                var tableRows = DbContext.Instance.SelectFromTable(ids, new(), table.TableName, table.DatabaseName!)
                    .Select(row => row.Value)
                    .ToList();

                return GetJoinedTableContent(tableRows, table.TableName, table.DatabaseName!);
            }

            return HandleConstantExpressionWithJoin(root);
        }

        var leftResult = Evaluate(root.Left!);
        var rightResult = Evaluate(root.Right!);

        if (root.Type == Node.NodeType.And)
        {
            return And(leftResult, rightResult);
        }

        if (root.Type == Node.NodeType.Or)
        {
            return Or(leftResult, rightResult);
        }

        throw new Exception("Invalid tree node type!");
    }

    public HashSet<string> EvaluateWithoutJoin(Node root)
    {
        if ((root.Type == Node.NodeType.Eq || root.Type == Node.NodeType.Operator) 
            &&  root.Left!.Type == Node.NodeType.Column && root.Right!.Type == Node.NodeType.Column)
        {
            return HandleTwoColumnExpression(root, fromTable!);
        }

        if ((root.Type == Node.NodeType.Operator || root.Type == Node.NodeType.Eq) 
            && root.Left!.Type == Node.NodeType.Value
            && root.Right!.Type == Node.NodeType.Column)
        {
            (root.Right, root.Left) = (root.Left, root.Right);

            switch (root.Value.ParsedValue)
            {
                case "<": root.Value.Value = ">"; break;
                case ">": root.Value.Value = "<"; break;
                case "<=": root.Value.Value = ">="; break;
                case ">=": root.Value.Value = "<="; break;
                default: break;
            }
        }

        if (root.Type == Node.NodeType.Eq)
        {
            if (root.Left!.Type == Node.NodeType.Column && root.Right!.Type == Node.NodeType.Value) 
            {
                return HandleIndexableStatement(root, fromTable!);
            }

            
        }

        if (root.Type == Node.NodeType.Operator)
        {
            if (root.Left!.Type == Node.NodeType.Column)
            {
                return HandleNonIndexableStatement(root, fromTable!);
            }

            return HandleConstantExpression(root);
        }

        var leftResult = EvaluateWithoutJoin(root.Left!);
        var rightResult = EvaluateWithoutJoin(root.Right!);
        
        if (root.Type == Node.NodeType.And)
        {
            return new HashSet<string>(leftResult.Intersect(rightResult));
        }
        
        if (root.Type == Node.NodeType.Or)
        {
            return new HashSet<string>(leftResult.Union(rightResult));
        }

        throw new Exception("Invalid tree node type!");
    }

    private HashSet<string> HandleIndexableStatement(Node root, TableDetail table)
    {
        string? leftValue = root.Left!.Value.ParsedValue;
        string? rightValue = root.Right!.Value.Value!.ToString();

        table.IndexedColumns!.TryGetValue(leftValue!, out string? indexFile);
        if (indexFile != null)
        {
            return DbContext.Instance.FilterUsingIndex(rightValue!, indexFile, table.TableName, table.DatabaseName!);
        }

        int columnIndex = table.PrimaryKeys!.IndexOf(leftValue!);
        if (columnIndex > -1)
        {
            return DbContext.Instance.FilterUsingPrimaryKey(rightValue!, columnIndex, table.TableName, table.DatabaseName!);
        }

        return table.TableContent!
            .Where(entry => entry.Value[root.Left!.Value.ParsedValue] == root.Right!.Value.ParsedValue)
            .Select(entry => entry.Key)
            .ToHashSet();
    }

    private HashSet<string> HandleNonIndexableStatement(Node root, TableDetail table)
    {
        string? leftValue = root.Left!.Value.ParsedValue;
        
        Func<KeyValuePair<string, Dictionary<string, dynamic>>, bool> pred = root.Value.ParsedValue switch
        {
            "!=" => entry => entry.Value[leftValue!] != root.Right!.Value.ParsedValue,
            "<" => entry => entry.Value[leftValue!] < root.Right!.Value.ParsedValue,
            ">" => entry => entry.Value[leftValue!] > root.Right!.Value.ParsedValue,
            "<=" => entry => entry.Value[leftValue!] <= root.Right!.Value.ParsedValue,
            ">=" => entry => entry.Value[leftValue!] >= root.Right!.Value.ParsedValue,
            _ => throw new SecurityException("Invalid operator")
        };

        return table.TableContent!
            .Where(pred)
            .Select(entry => entry.Key)
            .ToHashSet();
    }

    private HashSet<string> HandleTwoColumnExpression(Node root, TableDetail table)
    {
        string? leftValue = root.Left!.Value.ParsedValue;
        string? rightValue = root.Right!.Value.ParsedValue;
        
        Func<KeyValuePair<string, Dictionary<string, dynamic>>, bool> pred = root.Value.ParsedValue switch
        {
            "=" => entry => entry.Value[leftValue!] == entry.Value[rightValue!],
            "!=" => entry => entry.Value[leftValue!] != entry.Value[rightValue!],
            "<" => entry => entry.Value[leftValue!] < entry.Value[rightValue!],
            ">" => entry => entry.Value[leftValue!] > entry.Value[rightValue!],
            "<=" => entry => entry.Value[leftValue!] <= entry.Value[rightValue!],
            ">=" => entry => entry.Value[leftValue!] >= entry.Value[rightValue!],
            _ => throw new SecurityException("Invalid operator")
        };

        return table.TableContent!
            .Where(pred)
            .Select(entry => entry.Key)
            .ToHashSet();
    }

    private HashSet<string> HandleConstantExpression(Node root)
    {
        bool isCondTrue = root.Value.ParsedValue switch
        {
            "=" => root.Left!.Value.ParsedValue == root.Right!.Value.ParsedValue,
            "!=" => root.Left!.Value.ParsedValue != root.Right!.Value.ParsedValue,
            "<" => root.Left!.Value.ParsedValue < root.Right!.Value.ParsedValue,
            ">" => root.Left!.Value.ParsedValue > root.Right!.Value.ParsedValue,
            "<=" => root.Left!.Value.ParsedValue <= root.Right!.Value.ParsedValue,
            ">=" => root.Left!.Value.ParsedValue >= root.Right!.Value.ParsedValue,
            _ => throw new SecurityException("Invalid operator")
        };

        var content = fromTable!.TableContent!
            .Select(row => row.Key)
            .ToHashSet();

        return isCondTrue ? content : new();
    }

    private TableRows HandleConstantExpressionWithJoin(Node root)
    {
        bool isCondTrue = root.Value.ParsedValue switch
        {
            "=" => root.Left!.Value.ParsedValue == root.Right!.Value.ParsedValue,
            "!=" => root.Left!.Value.ParsedValue != root.Right!.Value.ParsedValue,
            "<" => root.Left!.Value.ParsedValue < root.Right!.Value.ParsedValue,
            ">" => root.Left!.Value.ParsedValue > root.Right!.Value.ParsedValue,
            "<=" => root.Left!.Value.ParsedValue <= root.Right!.Value.ParsedValue,
            ">=" => root.Left!.Value.ParsedValue >= root.Right!.Value.ParsedValue,
            _ => throw new SecurityException("Invalid operator")
        };

        if (isCondTrue)
        {
            return GetJoinedTableContent(fromTable!.TableContentValues!, fromTable.TableName, fromTable.DatabaseName!);
        }

        return new();
    }

    private TableRows GetJoinedTableContent(List<Dictionary<string, dynamic>> tableRows, string tableName, string databaseName)
    {
        var groupedInitialTable = tableRows
               .Select(row => new Dictionary<string, Dictionary<string, dynamic>> { { tableName, row } })
               .ToList();

        return join!.Evaluate(groupedInitialTable);
    }

    private static TableRows And(TableRows leftResult, TableRows rightResult)
    {
        return leftResult.Intersect(rightResult).ToList();
    }

    private static TableRows Or(TableRows leftResult, TableRows rightResult)
    {
        return leftResult.Union(rightResult).ToList();
    }
}