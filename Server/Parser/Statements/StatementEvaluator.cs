using Server.Models.Statement;

namespace Server.Parser.Statements
{
    internal class StatementEvaluator
    {
        public static Boolean Evaluate(Node node, Dictionary<string, object> data)
        {
            dynamic returnValue = EvaluateExpression(node, data);

            if (returnValue is bool || returnValue is Boolean) 
            {
                return returnValue;
            }

            throw new Exception("Error evaluating the statement! The result must a boolean value!");
        }

        private static dynamic EvaluateExpression(Node node, Dictionary<string, object> data)
        {
            return node.Type switch
            {
                Node.NodeType.Value => node.Value,
                Node.NodeType.Column => node.FromColumnToNodeValue(data),
                Node.NodeType.Operator => HandleOperator(node, data),
                _ => throw new Exception("Invalid node type: " + node.Type),
            };
        }

        private static Boolean HandleOperator(Node node, Dictionary<string, object> data) 
        {
            if (node.Left == null || node.Right == null)
            {
                return false;
            }

            EvaluateExpression(node.Left, data);
            EvaluateExpression(node.Right, data);

            string operatorStr = (string)node.Value.Value!;
            return node.Left.Compare(operatorStr, node.Right);
        }
    }
}
