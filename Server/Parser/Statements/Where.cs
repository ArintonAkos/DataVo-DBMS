using System.Data;
using Server.Models.Statement;
using Server.Server.MongoDB;

namespace Server.Parser.Statements;

internal class Where
{
    private readonly WhereModel _model;

    public Where(string match) => _model = WhereModel.FromString(match);

    // Ide sztem nem datarow kell csak betettem IDE ezt javasolta
    public List<DataRow> Evaluate(HashSet<string> indexedColumns)
    {
        Optimize(_model.Statement, indexedColumns);
        var evaluator = new StatementEvaluator(_model.Database, _model.Table);
        HashSet<string> rowKeys = evaluator.Evaluate(_model.Statement);

        return DbContext.Instance.GetRows(rowKeys, _model.Table, _model.Database);
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

    private static void CheckLeftAndRightHandOperand(Node? left, Node? right)
    {
        if (left == null || right == null)
        {
            throw new Exception("Invalid WHERE statement!");
        }
    }
}