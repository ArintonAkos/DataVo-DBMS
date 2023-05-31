using Server.Models.Statement.Utils;
using Server.Server.MongoDB;
using Server.Services;
using System.Security;

namespace Server.Parser.Statements;
using TableRows = Dictionary<string, Dictionary<string, Dictionary<string, dynamic>>>;

internal class StatementEvaluator
{
    private TableService tableService { get; set; }
    private Join? join { get; set; }
    private TableDetail? fromTable { get; set; } 

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
            return HandleTwoColumnExpression(root);
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
                return HandleIndexableStatement(root);
            }

            return HandleConstantExpression(root);
        }

        if (root.Type == Node.NodeType.Operator)
        {
            if (root.Left!.Type == Node.NodeType.Column)
            {
                return HandleNonIndexableStatement(root);
            }

            return HandleConstantExpression(root);
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

    private TableRows HandleIndexableStatement(Node root)
    {
        Tuple<TableDetail, string> parseResult = tableService.ParseAndFindTableDetailByColumn(root.Left!.Value.ParsedValue);
        
        TableDetail table = parseResult.Item1;
        string leftValue = parseResult.Item2;
        string? rightValue = root.Right!.Value.Value!.ToString();

        Dictionary<string, Dictionary<string, dynamic>> tableRows;

        table.IndexedColumns!.TryGetValue(leftValue!, out string? indexFile);
        if (indexFile != null)
        {
            List<string> ids = DbContext.Instance.FilterUsingIndex(rightValue!, indexFile, table.TableName, table.DatabaseName!).ToList();

            tableRows = DbContext.Instance.SelectFromTable(ids, new(), table.TableName, table.DatabaseName!);

            return GetJoinedTableContent(tableRows, table.TableName);
        }

        int columnIndex = table.PrimaryKeys!.IndexOf(leftValue!);
        if (columnIndex > -1)
        {
            List<string> ids = DbContext.Instance.FilterUsingPrimaryKey(rightValue!, columnIndex, table.TableName, table.DatabaseName!).ToList();

            tableRows = DbContext.Instance.SelectFromTable(ids, new(), table.TableName, table.DatabaseName!);

            return GetJoinedTableContent(tableRows, table.TableName);
        }

        tableRows = table.TableContent!
            .Where(entry => entry.Value[root.Left!.Value.ParsedValue] == root.Right!.Value.ParsedValue)
            .ToDictionary(t => t.Key, t => t.Value);

        return GetJoinedTableContent(tableRows, table.TableName);
    }

    private TableRows HandleNonIndexableStatement(Node root)
    {
        Tuple<TableDetail, string> parseResult = tableService.ParseAndFindTableDetailByColumn(root.Left!.Value.ParsedValue);
        
        TableDetail table = parseResult.Item1;
        string leftValue = parseResult.Item2;
        
        Func<KeyValuePair<string, Dictionary<string, dynamic>>, bool> pred = root.Value.ParsedValue switch
        {
            "=" => entry => entry.Value[leftValue!] == root.Right!.Value.ParsedValue,
            "!=" => entry => entry.Value[leftValue!] != root.Right!.Value.ParsedValue,
            "<" => entry => entry.Value[leftValue!] < root.Right!.Value.ParsedValue,
            ">" => entry => entry.Value[leftValue!] > root.Right!.Value.ParsedValue,
            "<=" => entry => entry.Value[leftValue!] <= root.Right!.Value.ParsedValue,
            ">=" => entry => entry.Value[leftValue!] >= root.Right!.Value.ParsedValue,
            _ => throw new SecurityException("Invalid operator")
        };

        Dictionary<string, Dictionary<string, dynamic>> tableRows = table.TableContent!
            .Where(pred)
            .ToDictionary(t => t.Key, t => t.Value);

        return GetJoinedTableContent(tableRows, table.TableName);
    }

    private TableRows HandleTwoColumnExpression(Node root)
    {
        Tuple<TableDetail, string> parseResult1 = tableService.ParseAndFindTableDetailByColumn(root.Left!.Value.ParsedValue);
        Tuple<TableDetail, string> parseResult2 = tableService.ParseAndFindTableDetailByColumn(root.Right!.Value.ParsedValue);

        TableDetail table = parseResult1.Item1;
        TableDetail rightTable = parseResult2.Item1;

        if (table.TableName != rightTable.TableName)
        {
            throw new SecurityException("Join like statement not permitted in where clause!");
        }

        string? leftValue = parseResult1.Item2;
        string? rightValue = parseResult2.Item2;
        
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

        Dictionary<string, Dictionary<string, dynamic>> tableRows = table.TableContent!
            .Where(pred)
            .ToDictionary(t => t.Key, t => t.Value);

        return GetJoinedTableContent(tableRows, table.TableName);
    }

    private TableRows HandleConstantExpression(Node root)
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
            return GetJoinedTableContent(fromTable!.TableContent!, fromTable.TableName);
        }

        return new();
    }

    private TableRows GetJoinedTableContent(Dictionary<string, Dictionary<string, dynamic>> tableRows, string tableName)
    {
        TableRows groupedInitialTable = new();

        foreach (var row in tableRows)
        {
            groupedInitialTable.Add(row.Key, new Dictionary<string, Dictionary<string, dynamic>> { { tableName, row.Value } });
        }

        return join!.Evaluate(groupedInitialTable);
    }

    private static TableRows And(TableRows leftResult, TableRows rightResult)
    {
        return leftResult.Keys.Intersect(rightResult.Keys)
               .ToDictionary(t => t, t => leftResult[t]);
    }

    private static TableRows Or(TableRows leftResult, TableRows rightResult)
    {
        HashSet<string> leftHashes = leftResult.Keys.ToHashSet();
        HashSet<string> rightHashes = rightResult.Keys.ToHashSet();

        HashSet<string> unionResult = new(leftHashes.Union(rightHashes));

        TableRows result = new();
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
}