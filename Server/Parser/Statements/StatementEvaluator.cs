using Server.Models.Statement;

namespace Server.Parser.Statements;

internal class StatementEvaluator
{
    private readonly string _databaseName;
    private readonly string _tableName;

    public StatementEvaluator(string databaseName, string tableName)
    {
        _databaseName = databaseName;
        _tableName = tableName;
    }

    public HashSet<string> Evaluate(Node root)
    {
        if (root.Type == Node.NodeType.Value || root.Type == Node.NodeType.Column)
        {
            return _databaseService.GetData(_tableName, root);
        }

        IEnumerable<Node> indexedSubtrees = root.GetIndexedSubtrees();
        List<string> combinedIndexes = _databaseService.GetCombinedIndexes(_tableName);

        HashSet<string> leftResult = Evaluate(root.Left);
        HashSet<string> rightResult = Evaluate(root.Right);

        // Use combined index to fetch data if possible
        if (root.Type == Node.NodeType.Operator && root.MatchesCombinedIndex(indexedSubtrees, combinedIndexes))
        {
            var indexedData = _databaseService.GetDataUsingIndex(_tableName, root);
            return indexedData;
        }

        // Merge results
        if (root.Value.Value == "AND")
        {
            return new HashSet<string>(leftResult.Intersect(rightResult));
        }

        if (root.Value.Value == "OR")
        {
            return new HashSet<string>(leftResult.Union(rightResult));
        }

        throw new Exception("Invalid node type!");
    }

    // ezt a kettot kellene kombinalni, mindkettoben van valami olyan dolog ami jol fog nekunk jonni

    // public HashSet<string> Evaluate(Node root)
    // {
    //     if (root.Type == Node.NodeType.Value || root.Type == Node.NodeType.Column)
    //     {
    //         return DbContext.Instance.GetRowsByIndex(root.Value.Value, _tableName, _databaseName);
    //     }
    //     else if (root.Type == Node.NodeType.Operator)
    //     {
    //         var leftResult = Evaluate(root.Left!);
    //         var rightResult = Evaluate(root.Right!);
    //
    //         switch (root.Value.Value)
    //         {
    //             case "AND":
    //                 return new HashSet<string>(leftResult.Intersect(rightResult));
    //             case "OR":
    //                 return new HashSet<string>(leftResult.Union(rightResult));
    //             default:
    //                 throw new Exception($"Invalid operator: {root.Value.Value}");
    //         }
    //     }
    //     else
    //     {
    //         throw new Exception($"Invalid node type: {root.Type}");
    //     }
    // }
}