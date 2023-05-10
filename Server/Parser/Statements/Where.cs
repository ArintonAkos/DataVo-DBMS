using Server.Models.Statement;
using Server.Server.MongoDB;

namespace Server.Parser.Statements;

internal class Where
{
    private readonly WhereModel _model;

    public Where(string match) => _model = WhereModel.FromString(match);

    public HashSet<string> Evaluate(HashSet<string> indexedColumns)
    {
        Optimize(_model.Statement, indexedColumns);
        return EvaluateNode(_model.Statement);
    }

    private HashSet<string> EvaluateNode(Node node)
    {
        HashSet<string> resultSet = new();

        if (node.Type == Node.NodeType.And)
        {
            HashSet<string> leftResultSet = EvaluateNode(node.Left);
            HashSet<string> rightResultSet = EvaluateNode(node.Right);

            resultSet.UnionWith(leftResultSet);
            resultSet.IntersectWith(rightResultSet);
        }
        else if (node.Type == Node.NodeType.Or)
        {
            HashSet<string> leftResultSet = EvaluateNode(node.Left);
            HashSet<string> rightResultSet = EvaluateNode(node.Right);

            resultSet.UnionWith(leftResultSet);
            resultSet.UnionWith(rightResultSet);
        }
        else if (node.Type == Node.NodeType.Column)
        {
            if (node.UseIndex)
            {
                // Fetch data from the index and add the corresponding row keys to the resultSet
                List<string> indexedRows =
                    DbContext.Instance.GetRowsByIndex(node.Value.Value, node.Value.Table, node.Value.Database);

                foreach (string rowKey in indexedRows)
                {
                    resultSet.Add(rowKey);
                }
            }
            else
            {
                // Fetch data from MongoDB directly using a full search query
                List<string> matchingRows =
                    DbContext.Instance.GetRowsByQuery(node.Value.Value, node.Value.Table, node.Value.Database);

                foreach (string rowKey in matchingRows)
                {
                    resultSet.Add(rowKey);
                }
            }
        }

        return resultSet;
    }

    private void Optimize(Node? node, HashSet<string> indexedColumns)
    {
        if (node == null)
        {
            return;
        }

        if (node.Type == Node.NodeType.Column && indexedColumns.Contains(node.Value.Value))
        {
            node.UseIndex = true;
        }
        else
        {
            node.UseIndex = false;
        }

        Optimize(node.Left, indexedColumns);
        Optimize(node.Right, indexedColumns);
    }
}