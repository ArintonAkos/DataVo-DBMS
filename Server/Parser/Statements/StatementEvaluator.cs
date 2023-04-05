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
                Node.NodeType.Value => bool.Parse(node.Value),
                Node.NodeType.Column => bool.Parse(data[node.Value].ToString()),
                Node.NodeType.Operator => node.Value switch
                {
                    "AND" => EvaluateExpression(node.Left, data) && EvaluateExpression(node.Right, data),
                    "OR" => EvaluateExpression(node.Left, data) || EvaluateExpression(node.Right, data),
                    "=" => EvaluateExpression(node.Left, data).Equals(EvaluateExpression(node.Right, data)),
                    "!=" => !EvaluateExpression(node.Left, data).Equals(EvaluateExpression(node.Right, data)),
                    ">" => double.Parse(EvaluateExpression(node.Left, data).ToString()) > double.Parse(EvaluateExpression(node.Right, data).ToString()),
                    "<" => double.Parse(EvaluateExpression(node.Left, data).ToString()) < double.Parse(EvaluateExpression(node.Right, data).ToString()),
                    ">=" => double.Parse(EvaluateExpression(node.Left, data).ToString()) >= double.Parse(EvaluateExpression(node.Right, data).ToString()),
                    "<=" => double.Parse(EvaluateExpression(node.Left, data).ToString()) <= double.Parse(EvaluateExpression(node.Right, data).ToString()),
                    "+" => double.Parse(EvaluateExpression(node.Left, data).ToString()) + double.Parse(EvaluateExpression(node.Right, data).ToString()),
                    "-" => double.Parse(EvaluateExpression(node.Left, data).ToString()) - double.Parse(EvaluateExpression(node.Right, data).ToString()),
                    "*" => double.Parse(EvaluateExpression(node.Left, data).ToString()) * double.Parse(EvaluateExpression(node.Right, data).ToString()),
                    "/" => double.Parse(EvaluateExpression(node.Left, data).ToString()) / double.Parse(EvaluateExpression(node.Right, data).ToString()),
                    "LEN" => EvaluateExpression(node.Left, data).ToString().Length,
                    "UPPER" => EvaluateExpression(node.Left, data).ToString().ToUpper(),
                    "LOWER" => EvaluateExpression(node.Left, data).ToString().ToLower(),
                    "NOT" => !EvaluateExpression(node.Right, data),
                    _ => throw new Exception("Invalid operator: " + node.Value),
                },
                _ => throw new Exception("Invalid node type: " + node.Type),
            };
        }
    }
}
