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

        if (root.Type != Node.NodeType.Or)
        {
            return root;
        }

        if (root.Left != null && root.Left.Type == Node.NodeType.And)
        {
            var leftChild = root.Left;
            var leftLeftChild = leftChild.Left;
            var leftRightChild = leftChild.Right;

            leftChild.Left = root;
            leftChild.Right = leftRightChild;
            root.Left = leftLeftChild;
            root = leftChild;
        }

        if (root.Right != null && root.Right.Type == Node.NodeType.And)
        {
            var rightChild = root.Right;
            var rightLeftChild = rightChild.Left;
            var rightRightChild = rightChild.Right;

            rightChild.Left = root;
            rightChild.Right = rightLeftChild;
            root.Right = rightRightChild;
            root = rightChild;
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