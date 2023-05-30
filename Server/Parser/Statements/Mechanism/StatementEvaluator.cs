using Server.Models.Statement.Utils;
using Server.Server.MongoDB;
using Server.Services;
using System.Security;

namespace Server.Parser.Statements;

internal class StatementEvaluator
{
    private TableService tableService { get; set; }
    private Join? joinStatements { get; set; }
    private TableDetail? singleTable 
    { 
        get
        {
            return tableService.TableDetails.FirstOrDefault().Value;
        } 
    }

    public StatementEvaluator(string databaseName, string tableName)
    {
        tableService = new TableService(databaseName);
        tableService.AddTableDetail(new TableDetail(tableName, null));
    }

    public StatementEvaluator(TableService tableService, Join joinStatements)
    {
        this.tableService = tableService;
        this.joinStatements = joinStatements;
    }

    public Dictionary<string, Dictionary<string, dynamic>> Evaluate(Node root)
    {
        if ((root.Type == Node.NodeType.Eq || root.Type == Node.NodeType.Operator)
            && root.Left!.Type == Node.NodeType.Column && root.Right!.Type == Node.NodeType.Column)
        {
            TableDetail leftTable = tableService.GetTableDetailByColumn(root.Left.Value.ParsedValue);
            TableDetail rightTable = tableService.GetTableDetailByColumn(root.Right.Value.ParsedValue);

            if (leftTable.TableName == rightTable.TableName)
            {
                throw new Exception("Join like statement not permitted in where clause!");
            }

            List<string> result = HandleTwoColumnExpression(root, leftTable).ToList();
            return DbContext.Instance.SelectFromTable(result, new(), leftTable.TableName, leftTable.DatabaseName!);
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
                List<string> result = HandleNonIndexableStatement(root, table).ToList();
                return DbContext.Instance.SelectFromTable(result, new(), table.TableName, table.DatabaseName!);
            }

            return new();
            // return HandleConstantExpression(root);
        }

        if (root.Type == Node.NodeType.Operator)
        {
            if (root.Left!.Type == Node.NodeType.Column)
            {
                TableDetail table = tableService.GetTableDetailByColumn(root.Left.Value.ParsedValue);
                List<string> result = HandleNonIndexableStatement(root, table).ToList();
                return DbContext.Instance.SelectFromTable(result, new(), table.TableName, table.DatabaseName!);
            }

            return new();
            // return HandleConstantExpression(root);
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

    public HashSet<string> EvaluateWithoutJoin(Node root)
    {
        if ((root.Type == Node.NodeType.Eq || root.Type == Node.NodeType.Operator) 
            &&  root.Left!.Type == Node.NodeType.Column && root.Right!.Type == Node.NodeType.Column)
        {
            return HandleTwoColumnExpression(root, singleTable!);
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
                return HandleIndexableStatement(root, singleTable!);
            }

            return HandleConstantExpression(root, singleTable!);
        }

        if (root.Type == Node.NodeType.Operator)
        {
            if (root.Left!.Type == Node.NodeType.Column)
            {
                return HandleNonIndexableStatement(root, singleTable!);
            }

            return HandleConstantExpression(root, singleTable!);
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

        return new HashSet<string>();
    }
}