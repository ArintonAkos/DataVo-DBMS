using Server.Models.Statement;

namespace Server.Parser.Statements;

internal class Where
{
    private readonly WhereModel _model;

    public Where(string match) => _model = WhereModel.FromString(match);

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

    private static void CheckLeftAndRightHandOperand(Node? left, Node? right)
    {
        if (left == null || right == null)
        {
            throw new Exception("Invalid WHERE statement!");
        }
    }
}