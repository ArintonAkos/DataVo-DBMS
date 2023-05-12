using Server.Models.Catalog;
using Server.Models.Statement;
using Server.Server.MongoDB;
using System.Security;

namespace Server.Parser.Statements;

internal class StatementEvaluator
{
    private readonly string _databaseName;
    private readonly string _tableName;
    private readonly List<string> _primaryKeys;
    private readonly Dictionary<string, string> _indexedColumns;
    
    private Dictionary<string, Dictionary<string, dynamic>>? _tableContentLoaded { get; set; }
    private Dictionary<string, Dictionary<string, dynamic>>? _tableContent 
    {
        get
        {
            _tableContentLoaded ??= DbContext.Instance.GetTableContents(_tableName, _databaseName);
            return _tableContentLoaded;
        }
    }

    public StatementEvaluator(string databaseName, string tableName)
    {
        _databaseName = databaseName;
        _tableName = tableName;
        _primaryKeys = Catalog.GetTablePrimaryKeys(tableName, databaseName);
        _indexedColumns = Catalog.GetTableIndexedColumns(tableName, databaseName);
        _tableContentLoaded = null;
    }

    public HashSet<string> Evaluate(Node root)
    {
        if ((root.Type == Node.NodeType.Eq || root.Type == Node.NodeType.Operator) 
            &&  root.Left!.Type == Node.NodeType.Column && root.Right!.Type == Node.NodeType.Column)
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
            return new HashSet<string>(leftResult.Intersect(rightResult));
        }
        
        if (root.Type == Node.NodeType.Or)
        {
            return new HashSet<string>(leftResult.Union(rightResult));
        }

        throw new Exception("Invalid tree node type!");
    }

    private HashSet<string> HandleIndexableStatement(Node root)
    {
        string? leftValue = root.Left!.Value.ParsedValue;
        string? rightValue = root.Right!.Value.Value!.ToString();

        _indexedColumns.TryGetValue(leftValue!, out string? indexFile);
        if (indexFile != null)
        {
            return DbContext.Instance.FilterUsingIndex(rightValue!, indexFile, _tableName, _databaseName);
        }

        int columnIndex = _primaryKeys.IndexOf(leftValue!);
        if (columnIndex > -1)
        {
            return DbContext.Instance.FilterUsingPrimaryKey(rightValue!, columnIndex, _tableName, _databaseName);
        }

        return _tableContent!
            .Where(entry => entry.Value[root.Left!.Value.ParsedValue] == root.Right!.Value.ParsedValue)
            .Select(entry => entry.Key)
            .ToHashSet();
    }

    private HashSet<string> HandleNonIndexableStatement(Node root)
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

        return _tableContent!
            .Where(pred)
            .Select(entry => entry.Key)
            .ToHashSet();
    }

    private HashSet<string> HandleTwoColumnExpression(Node root)
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

        return _tableContent!
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

        return isCondTrue
            ? _tableContent!.Keys.ToHashSet()
            : new HashSet<string>();
    }
}