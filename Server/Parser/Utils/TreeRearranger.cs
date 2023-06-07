using Server.Enums;
using Server.Models.Statement.Utils;

namespace Server.Parser.Utils;

public class TreeRearranger
{
    public static Node? Rearrange(Node? root)
    {
        if (root == null)
        {
            return null;
        }

        root.Left = Rearrange(root.Left);
        root.Right = Rearrange(root.Right);

        if (root.Type == Node.NodeType.And && root.Left != null && root.Right != null)
        {
            if (root.Left.Type == Node.NodeType.Or)
            {
                var A = root.Right;
                var B = root.Left.Left;
                var C = root.Left.Right;

                var newLeft = new Node { Left = A.Clone(), Right = B, Type = Node.NodeType.And };
                var newRight = new Node { Left = A.Clone(), Right = C, Type = Node.NodeType.And };

                root = new Node { Left = newLeft, Right = newRight, Type = Node.NodeType.Or };
            }

            if (root.Right.Type == Node.NodeType.Or)
            {
                var A = root.Left;
                var B = root.Right.Left;
                var C = root.Right.Right;

                var newLeft = new Node { Left = A.Clone(), Right = B, Type = Node.NodeType.And };
                var newRight = new Node { Left = C.Clone(), Right = A.Clone(), Type = Node.NodeType.And };

                root = new Node { Left = newLeft, Right = newRight, Type = Node.NodeType.Or };
            }
        }

        return root;
    }

    public static Node SimplifyAlgebraicExpressions(Node node)
    {
        if (node.Type == Node.NodeType.Value)
        {
            return node;
        }

        if (node.Left != null)
        {
            node.Left = SimplifyAlgebraicExpressions(node.Left);
        }

        if (node.Right != null)
        {
            node.Right = SimplifyAlgebraicExpressions(node.Right);
        }

        if (node.Value.ValueType != Node.NodeValueType.Operator)
        {
            return node;
        }

        if (node.Left?.Type == Node.NodeType.Value && node.Right?.Type == Node.NodeType.Value)
        {
            string @operator = node.Value.Value as string ?? throw new Exception("Invalid operator!");

            if (Operators.ArithmeticOperators.Contains(@operator))
            {
                return node.Left.HandleAlgebraicExpression(@operator, node.Right);
            }
        }

        return node;
    }
}