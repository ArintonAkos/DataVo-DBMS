using DataVo.Core.Enums;
using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using static DataVo.Core.Models.Statement.Utils.Node;

namespace DataVo.Core.Parser;

public class Parser
{
    private readonly List<Token> _tokens;
    private int _position;

    public Parser(List<Token> tokens)
    {
        _tokens = tokens;
        _position = 0;
    }

    private Token Current => _position < _tokens.Count ? _tokens[_position] : _tokens.Last();
    private Token Advance() => _position < _tokens.Count ? _tokens[_position++] : _tokens.Last();
    private bool IsEof() => Current.Type == TokenType.EOF;

    private bool Match(TokenType type, string? value = null)
    {
        if (IsEof()) return false;
        
        // Value check is case-insensitive for keywords
        if (Current.Type == type && (value == null || Current.Value.Equals(value, StringComparison.OrdinalIgnoreCase)))
        {
            Advance();
            return true;
        }

        return false;
    }

    private Token Consume(TokenType type, string expectedMessage)
    {
        if (Current.Type == type) return Advance();
        throw new Exception($"Parser Error: Expected {expectedMessage} but found {Current}.");
    }

    public List<SqlStatement> Parse()
    {
        var statements = new List<SqlStatement>();

        while (!IsEof())
        {
            if (Match(TokenType.Keyword, "SELECT"))
                statements.Add(ParseSelectStatement());
            else if (Match(TokenType.Keyword, "USE"))
                statements.Add(ParseUseStatement());
            else if (Match(TokenType.Keyword, "SHOW"))
                statements.Add(ParseShowStatement());
            else if (Match(TokenType.Keyword, "DESCRIBE"))
                statements.Add(ParseDescribeStatement());
            else if (Match(TokenType.Keyword, "GO"))
                statements.Add(new GoStatement());
            else if (Match(TokenType.Keyword, "CREATE"))
                statements.Add(ParseCreateStatement());
            else if (Match(TokenType.Keyword, "ALTER"))
                statements.Add(ParseAlterStatement());
            else if (Match(TokenType.Keyword, "DROP"))
                statements.Add(ParseDropStatement());
            else if (Match(TokenType.Keyword, "DELETE"))
                statements.Add(ParseDeleteStatement());
            else if (Match(TokenType.Keyword, "INSERT"))
                statements.Add(ParseInsertStatement());
            else
            {
                // Advance unknown tokens to avoid infinite loops
                Advance();
            }
        }

        return statements;
    }

    private UseStatement ParseUseStatement()
    {
        var dbNameToken = Consume(TokenType.Identifier, "database name");
        return new UseStatement { DatabaseName = new IdentifierNode(dbNameToken.Value) };
    }

    private SqlStatement ParseShowStatement()
    {
        if (Match(TokenType.Keyword, "DATABASES"))
            return new ShowDatabasesStatement();
        else if (Match(TokenType.Keyword, "TABLES"))
            return new ShowTablesStatement();
        
        throw new Exception("Parser Error: Expected DATABASES or TABLES after SHOW.");
    }

    private DescribeStatement ParseDescribeStatement()
    {
        var tableNameToken = Consume(TokenType.Identifier, "table name");
        return new DescribeStatement { TableName = new IdentifierNode(tableNameToken.Value) };
    }

    private SqlStatement ParseCreateStatement()
    {
        if (Match(TokenType.Keyword, "DATABASE"))
        {
            var dbNameToken = Consume(TokenType.Identifier, "database name");
            return new CreateDatabaseStatement { DatabaseName = new IdentifierNode(dbNameToken.Value) };
        }
        else if (Match(TokenType.Keyword, "TABLE"))
        {
            var stmt = new CreateTableStatement();
            var tableNameToken = Consume(TokenType.Identifier, "table name");
            stmt.TableName = new IdentifierNode(tableNameToken.Value);
            
            Consume(TokenType.Punctuation, "(");
            while (!IsEof() && !Match(TokenType.Punctuation, ")"))
            {
                var colDef = new ColumnDefinitionNode();
                colDef.ColumnName = new IdentifierNode(Consume(TokenType.Identifier, "column name").Value);
                
                Token typeToken = Advance();
                string typeStr = typeToken.Value;
                if (Match(TokenType.Punctuation, "("))
                {
                    typeStr += "(" + Advance().Value + ")";
                    Consume(TokenType.Punctuation, ")");
                }
                colDef.DataType = typeStr;

                while (!IsEof() && Current.Type != TokenType.Punctuation)
                {
                    if (Match(TokenType.Keyword, "PRIMARY"))
                    {
                        Consume(TokenType.Keyword, "KEY");
                        colDef.IsPrimaryKey = true;
                    }
                    else if (Match(TokenType.Keyword, "UNIQUE"))
                    {
                        colDef.IsUnique = true;
                    }
                    else if (Match(TokenType.Keyword, "REFERENCES"))
                    {
                        colDef.ReferencesTable = new IdentifierNode(Consume(TokenType.Identifier, "reference table name").Value);
                        Consume(TokenType.Punctuation, "(");
                        colDef.ReferencesColumn = new IdentifierNode(Consume(TokenType.Identifier, "reference column name").Value);
                        Consume(TokenType.Punctuation, ")");
                    }
                    else
                    {
                        break;
                    }
                }
                
                stmt.Columns.Add(colDef);
                
                if (Current.Type == TokenType.Punctuation && Current.Value == ",") Advance();
            }
            return stmt;
        }
        else if (Match(TokenType.Keyword, "INDEX"))
        {
            var stmt = new CreateIndexStatement();
            stmt.IndexName = new IdentifierNode(Consume(TokenType.Identifier, "index name").Value);
            Consume(TokenType.Keyword, "ON");
            stmt.TableName = new IdentifierNode(Consume(TokenType.Identifier, "table name").Value);
            Consume(TokenType.Punctuation, "(");
            stmt.ColumnName = new IdentifierNode(Consume(TokenType.Identifier, "column name").Value);
            Consume(TokenType.Punctuation, ")");
            return stmt;
        }
        throw new Exception("Parser Error: Unknown CREATE statement type.");
    }

    private SqlStatement ParseAlterStatement()
    {
        Consume(TokenType.Keyword, "TABLE");
        var tableNameToken = Consume(TokenType.Identifier, "table name");
        
        if (Match(TokenType.Keyword, "ADD"))
        {
            var stmt = new AlterTableAddColumnStatement { TableName = new IdentifierNode(tableNameToken.Value) };
            
            var colDef = new ColumnDefinitionNode();
            colDef.ColumnName = new IdentifierNode(Consume(TokenType.Identifier, "column name").Value);
            
            Token typeToken = Advance();
            string typeStr = typeToken.Value;
            if (Match(TokenType.Punctuation, "("))
            {
                typeStr += "(" + Advance().Value + ")";
                Consume(TokenType.Punctuation, ")");
            }
            colDef.DataType = typeStr;

            while (!IsEof() && Current.Type != TokenType.Punctuation && Current.Type != TokenType.EOF)
            {
                if (Match(TokenType.Keyword, "PRIMARY"))
                {
                    Consume(TokenType.Keyword, "KEY");
                    colDef.IsPrimaryKey = true;
                }
                else if (Match(TokenType.Keyword, "UNIQUE"))
                {
                    colDef.IsUnique = true;
                }
                else if (Match(TokenType.Keyword, "REFERENCES"))
                {
                    colDef.ReferencesTable = new IdentifierNode(Consume(TokenType.Identifier, "reference table name").Value);
                    Consume(TokenType.Punctuation, "(");
                    colDef.ReferencesColumn = new IdentifierNode(Consume(TokenType.Identifier, "reference column name").Value);
                    Consume(TokenType.Punctuation, ")");
                }
                else
                {
                    break;
                }
            }
            stmt.Column = colDef;
            return stmt;
        }
        else if (Match(TokenType.Keyword, "DROP"))
        {
            if (Current.Type == TokenType.Keyword && Current.Value.ToUpperInvariant() == "COLUMN") Advance();
            
            var stmt = new AlterTableDropColumnStatement { TableName = new IdentifierNode(tableNameToken.Value) };
            stmt.ColumnName = new IdentifierNode(Consume(TokenType.Identifier, "column name").Value);
            return stmt;
        }
        else if (Match(TokenType.Keyword, "MODIFY"))
        {
             return new AlterTableStatement { TableName = new IdentifierNode(tableNameToken.Value) };
        }
        
        throw new Exception("Parser Error: Unknown ALTER TABLE operation.");
    }

    private SqlStatement ParseDropStatement()
    {
        if (Match(TokenType.Keyword, "DATABASE"))
            return new DropDatabaseStatement { DatabaseName = new IdentifierNode(Consume(TokenType.Identifier, "database name").Value) };
        else if (Match(TokenType.Keyword, "TABLE"))
            return new DropTableStatement { TableName = new IdentifierNode(Consume(TokenType.Identifier, "table name").Value) };
        else if (Match(TokenType.Keyword, "INDEX"))
        {
            var stmt = new DropIndexStatement();
            stmt.IndexName = new IdentifierNode(Consume(TokenType.Identifier, "index name").Value);
            Consume(TokenType.Keyword, "ON");
            stmt.TableName = new IdentifierNode(Consume(TokenType.Identifier, "table name").Value);
            return stmt;
        }
        throw new Exception("Parser Error: Unknown DROP statement type.");
    }

    private SqlStatement ParseDeleteStatement()
    {
        Consume(TokenType.Keyword, "FROM");
        var stmt = new DeleteFromStatement();
        stmt.TableName = new IdentifierNode(Consume(TokenType.Identifier, "table name").Value);

        if (Match(TokenType.Keyword, "WHERE"))
        {
            var expressionTokens = new Queue<Token>();
            while (!IsEof())
            {
                expressionTokens.Enqueue(Advance());
            }
            stmt.WhereExpression = ParseWhereExpression(expressionTokens);
        }
        else
        {
            stmt.WhereExpression = new Node { Type = NodeType.Column, Value = NodeValue.RawString("1=1") };
        }
        return stmt;
    }

    private SqlStatement ParseInsertStatement()
    {
        Consume(TokenType.Keyword, "INTO");
        var stmt = new InsertIntoStatement();
        stmt.TableName = new IdentifierNode(Consume(TokenType.Identifier, "table name").Value);

        if (Match(TokenType.Punctuation, "("))
        {
            while (!IsEof() && !Match(TokenType.Punctuation, ")"))
            {
                stmt.Columns.Add(new IdentifierNode(Consume(TokenType.Identifier, "column name").Value));
                if (Current.Type == TokenType.Punctuation && Current.Value == ",") Advance();
            }
        }

        Consume(TokenType.Keyword, "VALUES");
        
        while (!IsEof())
        {
            if (Match(TokenType.Punctuation, "("))
            {
                var valuesList = new List<SqlNode>();
                while (!IsEof() && !Match(TokenType.Punctuation, ")"))
                {
                    Token valToken = Advance();
                    valuesList.Add(new IdentifierNode(valToken.Value)); 
                    
                    if (Current.Type == TokenType.Punctuation && Current.Value == ",") Advance();
                }
                stmt.ValuesLists.Add(valuesList);
            }
            
            if (Match(TokenType.Punctuation, ","))
                continue;
            else
                break;
        }

        return stmt;
    }

    private SelectStatement ParseSelectStatement()
    {
        var selectStmt = new SelectStatement();

        // 1. Parse Columns (SELECT already matched)
        selectStmt.Columns = ParseColumnList();

        // 2. Parse FROM
        Consume(TokenType.Keyword, "FROM");
        var tableNameToken = Consume(TokenType.Identifier, "table name");
        selectStmt.FromTable = new IdentifierNode(tableNameToken.Value);

        // 3. Parse optional JOINs
        while (IsJoinKeyword())
        {
            selectStmt.Joins.Add(ParseJoinDetail());
        }

        // 4. Parse optional WHERE
        if (Match(TokenType.Keyword, "WHERE"))
        {
            // Collect all tokens until EOF or the next major keyword (e.g. GROUP BY) 
            // and pass them to the Shunting-Yard expression parser
            var expressionTokens = new Queue<Token>();
            while (!IsEof() && !IsGroupByKeyword())
            {
                expressionTokens.Enqueue(Advance());
            }

            selectStmt.WhereExpression = ParseWhereExpression(expressionTokens);
        }

        // 5. Parse optional GROUP BY
        if (IsGroupByKeyword())
        {
            selectStmt.GroupByExpression = ParseGroupBy();
        }

        // 6. Parse optional HAVING
        if (Match(TokenType.Keyword, "HAVING"))
        {
            var expressionTokens = new Queue<Token>();
            while (!IsEof() && !IsOrderByKeyword())
            {
                expressionTokens.Enqueue(Advance());
            }

            selectStmt.HavingExpression = ParseWhereExpression(expressionTokens);
        }

        // 7. Parse optional ORDER BY
        if (IsOrderByKeyword())
        {
            selectStmt.OrderByExpression = ParseOrderBy();
        }

        return selectStmt;
    }

    private bool IsJoinKeyword()
    {
        if (IsEof()) return false;
        string val = Current.Value.ToUpperInvariant();
        return val == "JOIN" || val == "INNER" || val == "LEFT" || val == "RIGHT" || val == "OUTER" || val == "CROSS";
    }

    private JoinDetailNode ParseJoinDetail()
    {
        var joinNode = new JoinDetailNode();
        string joinType = "";

        // Collect prefixes like LEFT, RIGHT, INNER, OUTER
        while (!IsEof() && Current.Value.ToUpperInvariant() != "JOIN")
        {
            joinType += Advance().Value.ToUpperInvariant() + " ";
        }
        
        Consume(TokenType.Keyword, "JOIN");
        joinNode.JoinType = (joinType + "JOIN").Trim();

        var tableToken = Consume(TokenType.Identifier, "join table name");
        joinNode.TableName = new IdentifierNode(tableToken.Value);

        // Optional table alias (e.g., JOIN Users u OR JOIN Users AS u)
        if (Current.Type == TokenType.Identifier || (Current.Type == TokenType.Keyword && Current.Value.ToUpperInvariant() == "AS"))
        {
            if (Match(TokenType.Keyword, "AS")) 
            {
                // Consumed AS, next is identifier
            }
            if (Current.Type == TokenType.Identifier)
            {
                joinNode.Alias = new IdentifierNode(Advance().Value);
            }
        }

        // Parse ON condition
        if (Match(TokenType.Keyword, "ON"))
        {
            var condition = new JoinConditionNode();
            
            // Left Side: Table.Column
            condition.LeftTable = new IdentifierNode(Consume(TokenType.Identifier, "left table name").Value);
            Consume(TokenType.Punctuation, ".");
            condition.LeftColumn = new IdentifierNode(Consume(TokenType.Identifier, "left column name").Value);

            Consume(TokenType.Operator, "=");

            // Right Side: Table.Column
            condition.RightTable = new IdentifierNode(Consume(TokenType.Identifier, "right table name").Value);
            Consume(TokenType.Punctuation, ".");
            condition.RightColumn = new IdentifierNode(Consume(TokenType.Identifier, "right column name").Value);

            joinNode.Condition = condition;
        }

        return joinNode;
    }

    private bool IsGroupByKeyword()
    {
        if (_position + 1 >= _tokens.Count) return false;
        return Current.Type == TokenType.Keyword && Current.Value.ToUpperInvariant() == "GROUP" &&
               _tokens[_position + 1].Type == TokenType.Keyword && _tokens[_position + 1].Value.ToUpperInvariant() == "BY";
    }

    private GroupByNode ParseGroupBy()
    {
        Consume(TokenType.Keyword, "GROUP");
        Consume(TokenType.Keyword, "BY");

        var groupByNode = new GroupByNode();

        while (!IsEof() && Current.Type != TokenType.Keyword)
        {
            var colToken = Consume(TokenType.Identifier, "group by column name");
            groupByNode.Columns.Add(new IdentifierNode(colToken.Value));

            if (Match(TokenType.Punctuation, ","))
                continue;
            else
                break;
        }

        return groupByNode;
    }

    private bool IsOrderByKeyword()
    {
        if (_position + 1 >= _tokens.Count) return false;
        return Current.Type == TokenType.Keyword && Current.Value.ToUpperInvariant() == "ORDER" &&
               _tokens[_position + 1].Type == TokenType.Keyword && _tokens[_position + 1].Value.ToUpperInvariant() == "BY";
    }

    private OrderByNode ParseOrderBy()
    {
        Consume(TokenType.Keyword, "ORDER");
        Consume(TokenType.Keyword, "BY");

        var orderByNode = new OrderByNode();

        while (!IsEof() && Current.Type != TokenType.Keyword)
        {
            var colToken = Consume(TokenType.Identifier, "order by column name");
            var colNode = new OrderByColumnNode { Column = new IdentifierNode(colToken.Value) };

            // Optional ASC or DESC
            if (Current.Type == TokenType.Keyword)
            {
                string kw = Current.Value.ToUpperInvariant();
                if (kw == "ASC")
                {
                    colNode.IsAscending = true;
                    Advance();
                }
                else if (kw == "DESC")
                {
                    colNode.IsAscending = false;
                    Advance();
                }
            }

            orderByNode.Columns.Add(colNode);

            if (Match(TokenType.Punctuation, ","))
                continue;
            else
                break;
        }

        return orderByNode;
    }

    private List<SqlNode> ParseColumnList()
    {
        var columns = new List<SqlNode>();

        while (!IsEof() && !Match(TokenType.Keyword, "FROM"))
        {
            // The Match("FROM") inside the condition will consume FROM if true, meaning the next call is table.
            // But we actually want to Peek "FROM". Let's revert that and just Peak.
            if (Current.Type == TokenType.Keyword && Current.Value.Equals("FROM", StringComparison.OrdinalIgnoreCase))
            {
                break;
            }

            if (Current.Type == TokenType.Punctuation && Current.Value == "*")
            {
                columns.Add(new IdentifierNode("*"));
                Advance();
            }
            else if (Current.Type == TokenType.Identifier)
            {
                var identifier = Advance();
                columns.Add(new IdentifierNode(identifier.Value));
            }
            else
            {
                // Skip unexpected tokens in column list for now
                Advance();
                continue;
            }

            // Consume comma if present, otherwise assume end of column list
            Match(TokenType.Punctuation, ",");
        }

        return columns;
    }

    // This adapts the existing Shunting-Yard from StatementParser to use the Lexer's Tokens directly
    private Node ParseWhereExpression(Queue<Token> tokens)
    {
        Stack<Node> values = new();
        Stack<Token> operators = new();

        while (tokens.Any())
        {
            Token token = tokens.Dequeue();

            if (token.Type == TokenType.Punctuation && token.Value == "(")
            {
                operators.Push(token);
            }
            else if (token.Type == TokenType.Punctuation && token.Value == ")")
            {
                while (operators.Count > 0 && !(operators.Peek().Type == TokenType.Punctuation && operators.Peek().Value == "("))
                {
                    EvaluateTopOperator(values, operators);
                }
                operators.Pop(); // Remove the "("
            }
            else if (token.Type == TokenType.Operator)
            {
                while (operators.Count > 0 && 
                      !(operators.Peek().Type == TokenType.Punctuation && operators.Peek().Value == "(") &&
                      GetPrecedence(token.Value) <= GetPrecedence(operators.Peek().Value))
                {
                    EvaluateTopOperator(values, operators);
                }
                operators.Push(token);
            }
            else if (token.Type == TokenType.Identifier)
            {
                values.Push(new Node
                {
                    Type = NodeType.Column,
                    Value = NodeValue.RawString(token.Value)
                });
            }
            else if (token.Type == TokenType.StringLiteral || token.Type == TokenType.NumberLiteral)
            {
                // NumberLiteral might not be parsed correctly if NodeValue.Parse expects it, but NodeValue.Parse handles ints
                // Lexer returns strings like "'John'" or "42", which NodeValue.Parse naturally handles.
                values.Push(new Node
                {
                    Type = NodeType.Value,
                    Value = NodeValue.Parse(token.Value)
                });
            }
            else
            {
                // Skip unrecognized tokens in expression
            }
        }

        while (operators.Count > 0)
        {
            EvaluateTopOperator(values, operators);
        }

        return values.Any() ? values.Pop() : new Node { Type = NodeType.Column, Value = NodeValue.RawString("1=1") };
    }

    private void EvaluateTopOperator(Stack<Node> values, Stack<Token> operators)
    {
        var opToken = operators.Pop();
        string op = opToken.Value.ToUpperInvariant();

        // Shunting-Yard normally pops right then left
        var right = values.Count > 0 ? values.Pop() : null;
        var left = values.Count > 0 ? values.Pop() : null;

        var type = GetNodeType(op);

        Node node = new()
        {
            Type = type,
            Value = NodeValue.Operator(opToken.Value),
            Left = left,
            Right = right,
        };

        values.Push(node);
    }

    private static NodeType GetNodeType(string op)
    {
        return op switch
        {
            "AND" => NodeType.And,
            "OR" => NodeType.Or,
            "=" => NodeType.Eq,
            "!=" or ">" or "<" or ">=" or "<=" => NodeType.Operator,
            "+" or "-" or "*" or "/" => NodeType.Operator,
            _ => throw new Exception($"Invalid AST operator: {op}"),
        };
    }

    private static int GetPrecedence(string op)
    {
        return op.ToUpperInvariant() switch
        {
            "OR" => 1,
            "AND" => 2,
            "=" or "!=" or ">" or "<" or ">=" or "<=" => 3,
            "+" or "-" => 4,
            "*" or "/" => 5,
            _ => -1,
        };
    }
}
