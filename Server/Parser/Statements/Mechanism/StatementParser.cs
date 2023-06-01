using Server.Enums;
using Server.Models.Statement.Utils;
using Server.Parser.Utils;
using Server.Utils;
using static Server.Models.Statement.Utils.Node;

namespace Server.Parser.Statements;

public static class StatementParser
{
    /// <summary>
    /// Converts a raw SQL-like condition string into a tree-based data structure 
    /// for easier analysis and processing.
    /// </summary>
    /// <param name="input">A SQL-like condition string.</param>
    /// <returns>
    /// The root node of the tree-based data structure representing the
    /// input condition.
    /// </returns>
    public static Node Parse(string input)
    {
        Queue<string> tokens = Tokenize(input);

        var statementTree = ParseExpression(tokens);
        statementTree = TreeRearranger.Rearrange(statementTree)!;

        return TreeRearranger.SimplifyAlgebraicExpressions(statementTree);
    }

    /// <summary>
    /// Converts the raw SQL-like condition string into a queue of tokens. 
    /// Tokens can be operators, values, or column identifiers.
    /// </summary>
    /// <param name="input">A SQL-like condition string.</param>
    /// <returns>A queue of tokens derived from the input string.</returns>
    /// <exception cref="ArgumentException">
    /// Thrown when the input contains an 
    /// unexpected character or sequence.
    /// </exception>
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

                while (pos < input.Length && (char.IsLetterOrDigit(input[pos]) || input[pos] == '_' || input[pos] == '.'))
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

    /// <summary>
    /// Parses the queue of tokens and constructs a tree-based data structure,
    /// where each node is an operation (equality, inequality, logical and, logical or)
    /// or a value (constant, variable).
    /// </summary>
    /// <param name="tokens">A queue of tokens.</param>
    /// <returns>
    /// The root node of the tree-based data structure representing the input condition.
    /// </returns>
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

    /// <summary>
    /// Gets the node type of a given operator.
    /// </summary>
    /// <param name="op">The operator string.</param>
    /// <returns>
    /// The NodeType corresponding to the given operator string.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// Thrown when the operator is invalid.
    /// </exception>
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

    /// <summary>
    /// Determines whether a given token is a value.
    /// </summary>
    /// <param name="token">The token string.</param>
    /// <returns>
    /// True if the token is a value, otherwise false.
    /// </returns>
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

    /// <summary>
    /// Determines whether a given token is an operator.
    /// </summary>
    /// <param name="token">The token string.</param>
    /// <returns>
    /// True if the token is an operator, otherwise false.
    /// </returns>
    private static bool IsOperator(string token) => Operators.Supported().Contains(token);

    /// <summary>
    /// Gets the precedence of a given operator.
    /// </summary>
    /// <param name="op">The operator string.</param>
    /// <returns>
    /// The precedence of the operator as an integer. Higher number indicates higher precedence.
    /// </returns>
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