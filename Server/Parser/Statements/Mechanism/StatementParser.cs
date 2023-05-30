using Server.Enums;
using Server.Models.Statement.Utils;
using Server.Parser.Utils;
using Server.Utils;
using static Server.Models.Statement.Utils.Node;

namespace Server.Parser.Statements;

public static class StatementParser
{
    /// <summary>
    ///     This method parses a raw input string into a Queue of tokens.
    ///     Than with the token it creates a tree of nodes representing the
    ///     condition using the polish notation.
    ///     The tree is than rearranged to be more efficient and the root node is returned.
    /// </summary>
    /// <param name="input">The input string which will be parsed.</param>
    /// <returns>An object containing the </returns>
    public static Node Parse(string input)
    {
        Queue<string> tokens = Tokenize(input);

        var statementTree = ParseExpression(tokens);
        statementTree = TreeRearranger.Rearrange(statementTree)!;

        return TreeRearranger.SimplifyAlgebraicExpressions(statementTree);
    }

    /// <summary>
    ///     This function tokenizes the input string.
    ///     It splits the input data into different element with the same attribute.
    ///     For example for an input string "a = 1 and b > 2" it will return a queue of tokens
    ///     of ["a", "=", "1", "and", "b", ">", "2"]
    /// </summary>
    /// <param name="input">The input string that will be tokenized</param>
    /// <returns>A queue with the tokens</returns>
    /// <exception cref="ArgumentException">ArgumentException is thrown when an unexpected character is found.</exception>
    private static Queue<string> Tokenize(string input)
    {
        Queue<string> tokens = new();
        int pos = 0;

        while (pos < input.Length)
        {
            char c = input[pos];

            if (char.IsWhiteSpace(c))
            {
                pos++;
            }
            else if (c == '(' || c == ')')
            {
                tokens.Enqueue(c.ToString());
                pos++;
            }
            else if (Operators.ContainsOperator(input, pos, out int length))
            {
                tokens.Enqueue(input.Substring(pos, length).Serialize());
                pos += length;
            }
            else if (char.IsLetter(c))
            {
                string identifier = string.Empty;

                while (pos < input.Length && (char.IsLetterOrDigit(input[pos]) || input[pos] == '_'))
                {
                    identifier += input[pos];
                    pos++;
                }

                tokens.Enqueue(identifier);
            }
            else if (char.IsDigit(c))
            {
                string number = string.Empty;

                while (pos < input.Length && (char.IsDigit(input[pos]) || input[pos] == '.' || input[pos] == '/'))
                {
                    number += input[pos];
                    pos++;
                }

                tokens.Enqueue(number);
            }
            else if (c == '\'')
            {
                string str = c.ToString();
                pos++;

                while (pos < input.Length && input[pos] != '\'')
                {
                    if (input[pos] == '\\' && pos + 1 < input.Length && input[pos + 1] == '\'')
                    {
                        str += '\'';
                        pos += 2;
                    }
                    else
                    {
                        str += input[pos];
                        pos++;
                    }
                }

                if (pos >= input.Length || input[pos] != '\'')
                {
                    throw new ArgumentException("Unterminated string literal");
                }

                str += input[pos].ToString();
                pos++;
                tokens.Enqueue(str);
            }
            else
            {
                throw new ArgumentException($"Invalid character: {c}");
            }
        }

        return tokens;
    }

    private static Node ParseExpression(Queue<string> tokens)
    {
        Stack<Node> values = new();
        Stack<string> operators = new();

        while (tokens.Any())
        {
            string token = tokens.Dequeue();

            if (token == "(")
            {
                operators.Push(token);
            }
            else if (token == ")")
            {
                while (operators.Count > 0 && operators.Peek() != "(")
                {
                    string op = operators.Pop();
                    var right = values.Pop();
                    var left = values.Pop();
                    var type = GetNodeType(op);

                    Node node = new()
                    {
                        Type = type,
                        Value = NodeValue.Operator(op),
                        Left = left,
                        Right = right,
                    };

                    values.Push(node);
                }

                operators.Pop();
            }
            else if (IsOperator(token))
            {
                while (operators.Count > 0 && GetPrecedence(token) <= GetPrecedence(operators.Peek()))
                {
                    string op = operators.Pop();
                    var right = values.Pop();
                    var left = values.Pop();
                    var type = GetNodeType(op);

                    Node node = new()
                    {
                        Type = type,
                        Value = NodeValue.Operator(op),
                        Left = left,
                        Right = right,
                    };

                    values.Push(node);
                }

                operators.Push(token);
            }
            else if (IsValue(token))
            {
                Node node = new()
                {
                    Type = NodeType.Value,
                    Value = NodeValue.Parse(token),
                };
                values.Push(node);
            }
            else // Otherwise it is a column
            {
                Node node = new()
                {
                    Type = NodeType.Column,
                    Value = NodeValue.RawString(token),
                };
                values.Push(node);
            }
        }

        while (operators.Count > 0)
        {
            string op = operators.Pop();
            var right = values.Pop();
            var left = values.Pop();
            var type = GetNodeType(op);

            Node node = new()
            {
                Type = type,
                Value = NodeValue.Operator(op),
                Left = left,
                Right = right,
            };
            values.Push(node);
        }

        return values.Pop();
    }

    private static NodeType GetNodeType(string op)
    {
        string uppercaseOp = op.ToUpper();

        return uppercaseOp switch
        {
            "AND" => NodeType.And,
            "OR" => NodeType.Or,
            "=" => NodeType.Eq,
            "!=" or ">" or "<" or ">=" or "<=" => NodeType.Operator,
            "+" or "-" or "*" or "/" => NodeType.Operator,
            _ => throw new ArgumentException($"Invalid operator: {op}"),
        };
    }

    private static bool IsValue(string token)
    {
        if (token.StartsWith("'") && token.EndsWith("'"))
        {
            return true;
        }

        if (DateOnly.TryParse(token, out _))
        {
            return true;
        }

        if (bool.TryParse(token, out _))
        {
            return true;
        }

        if (int.TryParse(token, out _))
        {
            return true;
        }

        if (double.TryParse(token, out _))
        {
            return true;
        }

        return false;
    }

    private static bool IsOperator(string token) => Operators.Supported().Contains(token);

    private static int GetPrecedence(string op)
    {
        return op switch
        {
            "OR" => 1,
            "AND" => 2,
            "=" or "!=" or ">" or "<" or ">=" or "<=" => 3,
            "+" or "-" => 4,
            "*" or "/" => 5,
            // "LEN" or "UPPER" or "LOWER" => 6, //  or "NOT"
            _ => -1,
        };
    }
}