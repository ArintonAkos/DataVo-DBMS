using Microsoft.Extensions.Primitives;
using Server.Enums;
using Server.Models.Statement;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Server.Parser.Statements
{
    public class StatementParser
    {
        private static List<string> operators = new List<string>
        {
            "AND", "OR",
            "=", "!=", ">", "<", ">=", "<=",
            "+", "-", "*", "/", 
            "LEN", "UPPER", "LOWER",
            "NOT"
        };

        private static bool IsOperator(string token)
        {
            return operators.Contains(token);
        }

        private static int GetPrecedence(string op)
        {
            switch (op)
            {
                case "AND":
                case "OR":
                    return 1;
                case "=":
                case "!=":
                case ">":
                case "<":
                case ">=":
                case "<=":
                    return 2;
                case "+":
                case "-":
                    return 3;
                case "*":
                case "/":
                    return 4;
                case "LEN":
                case "UPPER":
                case "LOWER":
                case "NOT":
                    return 5;
                default:
                    return -1;
            }
        }

        /// <summary>
        /// This method parses a raw input string into a Queue of tokens.
        /// Than with the token it creates a tree of nodes representng the 
        /// condition using the polish notation.
        /// </summary>
        /// <param name="input">The input string which will be parsed.</param>
        /// <returns>An object containing the </returns>
        public static Node Parse(string input)
        {
            Queue<string> tokens = Tokenize(input);
            
            return ParseExpression(tokens);
        }

        /// <summary>
        /// This function tokenizes the input string. 
        /// It splits the input data into different element with the stame attribute.
        /// For example for an input string "a = 1 and b > 2" it will return a queue of tokens
        /// of ["a", "=", "1", "and", "b", ">", "2"]
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
                else if (c == '(' || c == ')' || Operators.LogicalOperators.Contains(input.Substring(pos, 2)))
                {
                    tokens.Enqueue(input.Substring(pos, 2));
                    pos += 2;
                }
                else if (Operators.LogicalOperators.Contains(c.ToString()))
                {
                    tokens.Enqueue(c.ToString());
                    pos++;
                }
                else if (Operators.ArithmeticOperators.Contains(c.ToString()))
                {
                    tokens.Enqueue(c.ToString());
                    pos++;
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
                    
                    while (pos < input.Length && char.IsDigit(input[pos]))
                    {
                        number += input[pos];
                        pos++;
                    }

                    tokens.Enqueue(number);
                }
                else if (c == '\'')
                {
                    string str = string.Empty;
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
                string token = tokens.Peek();

                if (token == "(")
                {
                    operators.Push(token);
                }
                else if (token == ")")
                {
                    while (operators.Count > 0 && operators.Peek() != "(")
                    {
                        string op = operators.Pop();
                        Node right = values.Pop();
                        Node left = values.Pop();

                        Node node = new()
                        {
                            Type = Node.NodeType.Operator,
                            Value = op,
                            Left = left,
                            Right = right
                        };

                        values.Push(node);
                    }

                    operators.Pop();

                    // Handle NOT operator following closing parenthesis
                    if (operators.Count > 0 && operators.Peek() == "NOT")
                    {
                        string op = operators.Pop();
                        Node right = values.Pop();

                        Node node = new()
                        {
                            Type = Node.NodeType.Operator,
                            Value = op,
                            Right = right
                        };

                        values.Push(node);
                    }
                }
                else if (IsOperator(token))
                {
                    while (operators.Count > 0 && GetPrecedence(token) <= GetPrecedence(operators.Peek()))
                    {
                        string op = operators.Pop();
                        Node right = values.Pop();
                        Node left = values.Pop();
                        Node node = new()
                        {
                            Type = Node.NodeType.Operator,
                            Value = op,
                            Left = left,
                            Right = right
                        };
                        values.Push(node);
                    }
                    operators.Push(token);
                }
                else if (IsColumnName(token))
                {
                    Node node = new Node
                    {
                        Type = Node.NodeType.Column,
                        Value = token
                    };
                    values.Push(node);
                }
                else
                {
                    Node node = new Node
                    {
                        Type = Node.NodeType.Value,
                        Value = token
                    };
                    values.Push(node);
                }
            }

            while (operators.Count > 0)
            {
                string op = operators.Pop();
                Node right = values.Pop();
                Node left = values.Pop();
                Node node = new Node
                {
                    Type = Node.NodeType.Operator,
                    Value = op,
                    Left = left,
                    Right = right
                };
                values.Push(node);
            }

            return values.Pop();
        }

        private static bool IsColumnName(string str)
        {
            // Column names can only contain letters, digits, and underscores
            return Regex.IsMatch(str, "^[a-zA-Z0-9_]+$");
        }

        //private Node ParseExpression(Queue<string> tokens)
        //{
        //    Node result = ParseTerm(tokens);

        //    while (tokens.Count > 0 && Operators.LogicalOperators.Contains(tokens.Peek()))
        //    {
        //        string op = tokens.Dequeue();
        //        Node term = ParseTerm(tokens);

        //        if (!result.IsComparableWith(term))
        //        {
        //            throw new ArgumentException($"Cannot compare {result.Type} with {term.Type}");
        //        }

        //        result = new Node(Operators.BinaryOperators.Find(op), result, term);
        //    }

        //    if (tokens.Count > 0 && tokens.Peek() == "NOT")
        //    {
        //        tokens.Dequeue();
        //        result = new Node(Operators.UnaryOperators["NOT"], result);
        //    }

        //    return result;
        //}

        //private Node ParseTerm(Queue<string> tokens)
        //{
        //    Node result = ParseFactor(tokens);

        //    while (tokens.Count > 0 && Operators.ArithmeticOperators.Contains(tokens.Peek()))
        //    {
        //        string op = tokens.Dequeue();
        //        Node right = ParseFactor(tokens);

        //        if (!IsNumeric(result) || !IsNumeric(right))
        //        {
        //            throw new ArgumentException("Arithmetic operators can only be applied to numeric types");
        //        }

        //        result = new Node(Operators.BinaryOperators[op], result, right);
        //    }

        //    return result;
        //}

        //private Node ParseFactor(Queue<string> tokens)
        //{
        //    if (tokens.Count == 0)
        //    {
        //        throw new ArgumentException("Unexpected end of input");
        //    }

        //    string token = tokens.Dequeue();

        //    if (token == "(")
        //    {
        //        Node result = ParseExpression(tokens);

        //        if (tokens.Count == 0 || tokens.Dequeue() != ")")
        //        {
        //            throw new ArgumentException("Unmatched parentheses");
        //        }

        //        return result;
        //    }
        //    else if (IsLiteral(token))
        //    {
        //        return new Node(NodeType.Literal, token);
        //    }
        //    else if (IsColumn(token))
        //    {
        //        return new Node(NodeType.Column, token);
        //    }
        //    else if (Operators.ArithmeticOperators.Contains(token))
        //    {
        //        Node right = ParseFactor(tokens);

        //        if (!IsNumeric(result) || !IsNumeric(right))
        //        {
        //            throw new ArgumentException("Arithmetic operators can only be applied to numeric types");
        //        }

        //        return new Node(NodeType.Binary, token, result, right);
        //    }
        //    else if (Operators.FunctionOperators.Contains(token))
        //    {
        //        if (tokens.Count == 0 || tokens.Dequeue() != "(")
        //        {
        //            throw new ArgumentException("Function arguments must be enclosed in parentheses");
        //        }

        //        List<Node> args = new()
        //        {
        //            ParseExpression(tokens)
        //        };

        //        while (tokens.Count > 0 && tokens.Peek() == ",")
        //        {
        //            tokens.Dequeue();
        //            args.Add(ParseExpression(tokens));
        //        }

        //        if (tokens.Count == 0 || tokens.Dequeue() != ")")
        //        {
        //            throw new ArgumentException("Unmatched parentheses in function arguments");
        //        }

        //        return new Node(NodeType.Function, token, args.ToArray());
        //    }
        //    else if (token == "NOT")
        //    {
        //        Node operand = ParseFactor(tokens);
        //        return new Node(NodeType.Unary, "NOT", operand);
        //    }
        //    else
        //    {
        //        throw new ArgumentException($"Invalid token '{token}'");
        //    }
        //}

        //private Node ParsePrimary(Queue<string> tokens)
        //{
        //    string token = tokens.Peek();

        //    if (token == "(")
        //    {
        //        // Consume the opening parenthesis
        //        tokens.Dequeue();

        //        // Parse the subexpression inside the parentheses
        //        Node node = ParseExpression(tokens);

        //        // Check if there's a closing parenthesis
        //        if (tokens.Peek() != ")")
        //        {
        //            throw new ArgumentException("Expected ')' after subexpression.");
        //        }

        //        // Consume the closing parenthesis
        //        tokens.Dequeue();

        //        return node;
        //    }
        //    else if (token == "NOT")
        //    {
        //        // Consume the "NOT" keyword
        //        tokens.Dequeue();

        //        // Parse the subexpression after the "NOT" keyword
        //        Node node = ParsePrimary(tokens);

        //        // Create a new "NOT" node with the parsed subexpression as its child
        //        return new Node("NOT", node);
        //    }
        //    else if (IsNumeric(token))
        //    {
        //        // Parse a numeric literal and return a corresponding value node
        //        return new Node(double.Parse(token));
        //    }
        //    else if (IsStringLiteral(token))
        //    {
        //        // Parse a string literal and return a corresponding value node
        //        return new Node(token.Trim('\''));
        //    }
        //    else if (IsIdentifier(token))
        //    {
        //        // Check if the identifier is a function call
        //        if (tokens.Count > 1 && tokens.Skip(1).FirstOrDefault() == "(")
        //        {
        //            return ParseFunctionCall(tokens);
        //        }
        //        else
        //        {
        //            // Create a new identifier node with the parsed name
        //            return new Node(token);
        //        }
        //    }
        //    else
        //    {
        //        throw new ArgumentException($"Unexpected token '{token}'.");
        //    }
        //}

        //private static bool IsLiteral(string token)
        //{
        //    return double.TryParse(token, out double _) || IsStringLiteral(token);
        //}

        //private static bool IsStringLiteral(string token)
        //{
        //    return token.Length >= 2 && token[0] == '\'' && token[token.Length - 1] == '\'';
        //}

        //private static bool IsColumn(string token)
        //{
        //    return Regex.IsMatch(token, @"^(\w+\.)?\w+$", RegexOptions.IgnoreCase);
        //}

        //private static bool IsNumeric(string value)
        //{
        //    return double.TryParse(value, out double _);
        //}
    }
}
