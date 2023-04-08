using Amazon.Runtime.Internal.Util;
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
            switch (node.Type)
            {
                case Node.NodeType.Value:
                    return node.Value;
                case Node.NodeType.Column:
                    return node.FromColumnToNodeValue(data).Value;
                case Node.NodeType.Operator:
                    Node.NodeValue leftNodeValue = EvaluateExpression(node.Left!, data);
                    Node.NodeValue rightNodeValue = EvaluateExpression(node.Right!, data);
                    string op = (string)node.Value.Value!;

                    return leftNodeValue.Compare(op, rightNodeValue);
            };

            throw new Exception("Invalid node type!");
        }
    }
}
