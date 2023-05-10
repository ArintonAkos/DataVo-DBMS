using Server.Models.Statement;
using Server.Utils;
using System.Linq;

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

        var indexedSubtrees = root.GetIndexedSubtrees();
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
        else if (root.Value.Value == "OR")
        {
            return new HashSet<string>(leftResult.Union(rightResult));
        }

        throw new Exception("Invalid node type!");
    }
}