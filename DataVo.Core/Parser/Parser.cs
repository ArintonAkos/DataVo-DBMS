using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.Exceptions;
using DataVo.Core.Enums;
using DataVo.Core.Constants;
using DataVo.Core.Parser.Utils;

namespace DataVo.Core.Parser;

public class Parser(List<Token> tokens)
{
    // The position of the current token being parsed. Initialized to 0, meaning we start parsing from the first token in the list.
    private int _position = 0;

    // Pointer to the current token being parsed. If _position is at the end of the list, Current will return an EOF token.
    private Token Current => _position < tokens.Count ? tokens[_position] : tokens.Last();
    // Peek at the next token without advancing the position. Returns an EOF token if peeking beyond the end of the list.
    private Token Advance() => _position < tokens.Count ? tokens[_position++] : tokens.Last();
    // Helper method to check if we've reached the end of the token list
    private bool IsEof() => Current.Type == TokenType.EOF;

    /// <summary>
    /// Helper method to match the current token against an expected type and optional value. 
    /// If the token matches, it advances the position and returns true. Otherwise, 
    /// it returns false without advancing.
    /// </summary> 
    /// <param name="type">The expected token type to match.</param>
    /// <param name="value">An optional expected token value to match (case-insensitive). If null, only the type is checked.</param>
    /// <returns>True if the current token matches the expected type and value (if provided), false otherwise.</returns>
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
        throw new ParserException($"Parser Error: Expected {expectedMessage} but found {Current}.");
    }

    public List<SqlStatement> Parse()
    {
        var statements = new List<SqlStatement>();

        while (!IsEof())
        {
            if (Match(TokenType.Keyword, SqlKeywords.SELECT))
                statements.Add(ParseSelectStatement());
            else if (Match(TokenType.Keyword, SqlKeywords.USE))
                statements.Add(ParseUseStatement());
            else if (Match(TokenType.Keyword, SqlKeywords.SHOW))
                statements.Add(ParseShowStatement());
            else if (Match(TokenType.Keyword, SqlKeywords.DESCRIBE))
                statements.Add(ParseDescribeStatement());
            else if (Match(TokenType.Keyword, SqlKeywords.GO))
                statements.Add(new GoStatement());
            else if (Match(TokenType.Keyword, SqlKeywords.CREATE))
                statements.Add(ParseCreateStatement());
            else if (Match(TokenType.Keyword, SqlKeywords.ALTER))
                statements.Add(ParseAlterStatement());
            else if (Match(TokenType.Keyword, SqlKeywords.DROP))
                statements.Add(ParseDropStatement());
            else if (Match(TokenType.Keyword, SqlKeywords.DELETE))
                statements.Add(ParseDeleteStatement());
            else if (Match(TokenType.Keyword, SqlKeywords.INSERT))
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
        if (Match(TokenType.Keyword, SqlKeywords.DATABASES))
            return new ShowDatabasesStatement();
        else if (Match(TokenType.Keyword, SqlKeywords.TABLES))
            return new ShowTablesStatement();

        throw new ParserException("Parser Error: Expected DATABASES or TABLES after SHOW.");
    }

    private DescribeStatement ParseDescribeStatement()
    {
        var tableNameToken = Consume(TokenType.Identifier, "table name");
        return new DescribeStatement { TableName = new IdentifierNode(tableNameToken.Value) };
    }

    private SqlStatement ParseCreateStatement()
    {
        if (Match(TokenType.Keyword, SqlKeywords.DATABASE))
        {
            var dbNameToken = Consume(TokenType.Identifier, "database name");
            return new CreateDatabaseStatement { DatabaseName = new IdentifierNode(dbNameToken.Value) };
        }
        else if (Match(TokenType.Keyword, SqlKeywords.TABLE))
        {
            var stmt = new CreateTableStatement();
            var tableNameToken = Consume(TokenType.Identifier, "table name");
            stmt.TableName = new IdentifierNode(tableNameToken.Value);

            Consume(TokenType.Punctuation, SqlPunctuation.OpenParenToken);
            while (!IsEof() && !Match(TokenType.Punctuation, SqlPunctuation.CloseParenToken))
            {
                var colDef = new ColumnDefinitionNode();
                colDef.ColumnName = new IdentifierNode(Consume(TokenType.Identifier, "column name").Value);

                Token typeToken = Advance();
                string typeStr = typeToken.Value;
                if (Match(TokenType.Punctuation, SqlPunctuation.OpenParenToken))
                {
                    typeStr += "(" + Advance().Value + ")";
                    Consume(TokenType.Punctuation, SqlPunctuation.CloseParenToken);
                }
                colDef.DataType = typeStr;

                while (!IsEof() && Current.Type != TokenType.Punctuation)
                {
                    if (Match(TokenType.Keyword, SqlKeywords.PRIMARY))
                    {
                        Consume(TokenType.Keyword, SqlKeywords.KEY);
                        colDef.IsPrimaryKey = true;
                    }
                    else if (Match(TokenType.Keyword, SqlKeywords.UNIQUE))
                    {
                        colDef.IsUnique = true;
                    }
                    else if (Match(TokenType.Keyword, SqlKeywords.REFERENCES))
                    {
                        colDef.ReferencesTable = new IdentifierNode(Consume(TokenType.Identifier, "reference table name").Value);
                        Consume(TokenType.Punctuation, SqlPunctuation.OpenParenToken);
                        colDef.ReferencesColumn = new IdentifierNode(Consume(TokenType.Identifier, "reference column name").Value);
                        Consume(TokenType.Punctuation, SqlPunctuation.CloseParenToken);
                    }
                    else
                    {
                        break;
                    }
                }

                stmt.Columns.Add(colDef);

                if (Current.Type == TokenType.Punctuation && Current.Value == SqlPunctuation.CommaToken) Advance();
            }
            return stmt;
        }
        else if (Match(TokenType.Keyword, SqlKeywords.INDEX))
        {
            var stmt = new CreateIndexStatement();
            stmt.IndexName = new IdentifierNode(Consume(TokenType.Identifier, "index name").Value);
            Consume(TokenType.Keyword, SqlKeywords.ON);
            stmt.TableName = new IdentifierNode(Consume(TokenType.Identifier, "table name").Value);
            Consume(TokenType.Punctuation, SqlPunctuation.OpenParenToken);
            stmt.ColumnName = new IdentifierNode(Consume(TokenType.Identifier, "column name").Value);
            Consume(TokenType.Punctuation, SqlPunctuation.CloseParenToken);
            return stmt;
        }
        throw new ParserException("Parser Error: Unknown CREATE statement type.");
    }

    private SqlStatement ParseAlterStatement()
    {
        Consume(TokenType.Keyword, SqlKeywords.TABLE);
        var tableNameToken = Consume(TokenType.Identifier, "table name");

        if (Match(TokenType.Keyword, SqlKeywords.ADD))
        {
            var stmt = new AlterTableAddColumnStatement { TableName = new IdentifierNode(tableNameToken.Value) };

            var colDef = new ColumnDefinitionNode();
            colDef.ColumnName = new IdentifierNode(Consume(TokenType.Identifier, "column name").Value);

            Token typeToken = Advance();
            string typeStr = typeToken.Value;
            if (Match(TokenType.Punctuation, SqlPunctuation.OpenParenToken))
            {
                typeStr += "(" + Advance().Value + ")";
                Consume(TokenType.Punctuation, SqlPunctuation.CloseParenToken);
            }
            colDef.DataType = typeStr;

            while (!IsEof() && Current.Type != TokenType.Punctuation && Current.Type != TokenType.EOF)
            {
                if (Match(TokenType.Keyword, SqlKeywords.PRIMARY))
                {
                    Consume(TokenType.Keyword, SqlKeywords.KEY);
                    colDef.IsPrimaryKey = true;
                }
                else if (Match(TokenType.Keyword, SqlKeywords.UNIQUE))
                {
                    colDef.IsUnique = true;
                }
                else if (Match(TokenType.Keyword, SqlKeywords.REFERENCES))
                {
                    colDef.ReferencesTable = new IdentifierNode(Consume(TokenType.Identifier, "reference table name").Value);
                    Consume(TokenType.Punctuation, SqlPunctuation.OpenParenToken);
                    colDef.ReferencesColumn = new IdentifierNode(Consume(TokenType.Identifier, "reference column name").Value);
                    Consume(TokenType.Punctuation, SqlPunctuation.CloseParenToken);
                }
                else
                {
                    break;
                }
            }
            stmt.Column = colDef;
            return stmt;
        }
        else if (Match(TokenType.Keyword, SqlKeywords.DROP))
        {
            if (ParserSyntaxHelper.IsKeyword(Current, SqlKeywords.COLUMN)) Advance();

            var stmt = new AlterTableDropColumnStatement { TableName = new IdentifierNode(tableNameToken.Value) };
            stmt.ColumnName = new IdentifierNode(Consume(TokenType.Identifier, "column name").Value);
            return stmt;
        }
        else if (Match(TokenType.Keyword, SqlKeywords.MODIFY))
        {
            return new AlterTableStatement { TableName = new IdentifierNode(tableNameToken.Value) };
        }

        throw new ParserException("Parser Error: Unknown ALTER TABLE operation.");
    }

    private SqlStatement ParseDropStatement()
    {
        if (Match(TokenType.Keyword, SqlKeywords.DATABASE))
            return new DropDatabaseStatement { DatabaseName = new IdentifierNode(Consume(TokenType.Identifier, "database name").Value) };
        else if (Match(TokenType.Keyword, SqlKeywords.TABLE))
            return new DropTableStatement { TableName = new IdentifierNode(Consume(TokenType.Identifier, "table name").Value) };
        else if (Match(TokenType.Keyword, SqlKeywords.INDEX))
        {
            var stmt = new DropIndexStatement();
            stmt.IndexName = new IdentifierNode(Consume(TokenType.Identifier, "index name").Value);
            Consume(TokenType.Keyword, SqlKeywords.ON);
            stmt.TableName = new IdentifierNode(Consume(TokenType.Identifier, "table name").Value);
            return stmt;
        }
        throw new ParserException("Parser Error: Unknown DROP statement type.");
    }

    private SqlStatement ParseDeleteStatement()
    {
        Consume(TokenType.Keyword, SqlKeywords.FROM);
        var stmt = new DeleteFromStatement
        {
            TableName = new IdentifierNode(Consume(TokenType.Identifier, "table name").Value)
        };

        if (Match(TokenType.Keyword, SqlKeywords.WHERE))
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
            stmt.WhereExpression = new LiteralNode { Value = SqlLiterals.TrueExpression };
        }
        return stmt;
    }

    private SqlStatement ParseInsertStatement()
    {
        Consume(TokenType.Keyword, SqlKeywords.INTO);
        var stmt = new InsertIntoStatement();
        stmt.TableName = new IdentifierNode(Consume(TokenType.Identifier, "table name").Value);

        if (Match(TokenType.Punctuation, SqlPunctuation.OpenParenToken))
        {
            while (!IsEof() && !Match(TokenType.Punctuation, SqlPunctuation.CloseParenToken))
            {
                stmt.Columns.Add(new IdentifierNode(Consume(TokenType.Identifier, "column name").Value));
                if (Current.Type == TokenType.Punctuation && Current.Value == SqlPunctuation.CommaToken) Advance();
            }
        }

        Consume(TokenType.Keyword, SqlKeywords.VALUES);

        while (!IsEof())
        {
            if (Match(TokenType.Punctuation, SqlPunctuation.OpenParenToken))
            {
                var valuesList = new List<SqlNode>();
                while (!IsEof() && !Match(TokenType.Punctuation, SqlPunctuation.CloseParenToken))
                {
                    Token valToken = Advance();
                    valuesList.Add(new IdentifierNode(valToken.Value));

                    if (Current.Type == TokenType.Punctuation && Current.Value == SqlPunctuation.CommaToken) Advance();
                }
                stmt.ValuesLists.Add(valuesList);
            }

            if (Match(TokenType.Punctuation, SqlPunctuation.CommaToken))
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
        Consume(TokenType.Keyword, SqlKeywords.FROM);
        var tableNameToken = Consume(TokenType.Identifier, "table name");
        selectStmt.FromTable = new IdentifierNode(tableNameToken.Value);

        // Optional table alias (e.g., FROM Users u OR FROM Users AS u)
        if (Current.Type == TokenType.Identifier || Match(TokenType.Keyword, SqlKeywords.AS))
        {
            if (Current.Type == TokenType.Identifier)
            {
                selectStmt.FromAlias = new IdentifierNode(Advance().Value);
            }
        }

        // 3. Parse optional JOINs
        while (IsJoinKeyword())
        {
            selectStmt.Joins.Add(ParseJoinDetail());
        }

        // 4. Parse optional WHERE
        if (Match(TokenType.Keyword, SqlKeywords.WHERE))
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
        if (Match(TokenType.Keyword, SqlKeywords.HAVING))
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
        return !IsEof() && ParserSyntaxHelper.IsJoinKeyword(Current);
    }

    private JoinDetailNode ParseJoinDetail()
    {
        var joinNode = new JoinDetailNode();
        List<string> joinTypePrefixTokens = [];

        // Collect prefixes like LEFT, RIGHT, INNER, OUTER
        while (!IsEof() && !ParserSyntaxHelper.IsKeyword(Current, SqlKeywords.JOIN))
        {
            joinTypePrefixTokens.Add(Advance().Value.ToUpperInvariant());
        }

        Consume(TokenType.Keyword, SqlKeywords.JOIN);
        joinNode.JoinType = ParserSyntaxHelper.ResolveJoinType(joinTypePrefixTokens);

        var tableToken = Consume(TokenType.Identifier, "join table name");
        joinNode.TableName = new IdentifierNode(tableToken.Value);

        // Optional table alias (e.g., JOIN Users u OR JOIN Users AS u)
        if (Current.Type == TokenType.Identifier || ParserSyntaxHelper.IsKeyword(Current, SqlKeywords.AS))
        {
            if (Match(TokenType.Keyword, SqlKeywords.AS))
            {
                // Consumed AS, next is identifier
            }
            if (Current.Type == TokenType.Identifier)
            {
                joinNode.Alias = new IdentifierNode(Advance().Value);
            }
        }

        // Parse ON condition
        if (Match(TokenType.Keyword, SqlKeywords.ON))
        {
            var condition = new JoinConditionNode
            {
                // Left Side: Table.Column or Alias.Column or Column
                Left = ParseColumnRef("left")
            };

            // Expect an operator, currently only supports equals for JOIN conditions
            Consume(TokenType.Operator, Operators.EQUALS);

            // Right Side: Table.Column
            condition.Right = ParseColumnRef("right");

            joinNode.Condition = condition;
        }

        return joinNode;
    }

    private bool IsGroupByKeyword()
    {
        return ParserSyntaxHelper.IsGroupByAt(tokens, _position);
    }

    private GroupByNode ParseGroupBy()
    {
        Consume(TokenType.Keyword, SqlKeywords.GROUP);
        Consume(TokenType.Keyword, SqlKeywords.BY);

        var groupByNode = new GroupByNode();

        while (!IsEof() && Current.Type != TokenType.Keyword)
        {
            var colToken = Consume(TokenType.Identifier, "group by column name");
            groupByNode.Columns.Add(new IdentifierNode(colToken.Value));

            if (Match(TokenType.Punctuation, SqlPunctuation.CommaToken))
                continue;
            else
                break;
        }

        return groupByNode;
    }

    private bool IsOrderByKeyword()
    {
        return ParserSyntaxHelper.IsOrderByAt(tokens, _position);
    }

    private OrderByNode ParseOrderBy()
    {
        Consume(TokenType.Keyword, SqlKeywords.ORDER);
        Consume(TokenType.Keyword, SqlKeywords.BY);

        var orderByNode = new OrderByNode();

        while (!IsEof() && Current.Type != TokenType.Keyword)
        {
            var colToken = Consume(TokenType.Identifier, "order by column name");
            var colNode = new OrderByColumnNode { Column = new IdentifierNode(colToken.Value) };

            // Optional ASC or DESC
            if (Current.Type == TokenType.Keyword)
            {
                if (ParserSyntaxHelper.IsKeyword(Current, SqlKeywords.ASC))
                {
                    colNode.IsAscending = true;
                    Advance();
                }
                else if (ParserSyntaxHelper.IsKeyword(Current, SqlKeywords.DESC))
                {
                    colNode.IsAscending = false;
                    Advance();
                }
            }

            orderByNode.Columns.Add(colNode);

            if (Match(TokenType.Punctuation, SqlPunctuation.CommaToken))
                continue;
            else
                break;
        }

        return orderByNode;
    }

    private ColumnRefNode ParseColumnRef(string sideLabel)
    {
        var token = Consume(TokenType.Identifier, $"{sideLabel} column reference (e.g., Table.Column or Alias.Column or Column)");
        var parts = token.Value.Split('.');

        if (parts.Length == 1)
        {
            return new ColumnRefNode { TableOrAlias = null, Column = parts[0] };
        }
        else if (parts.Length == 2)
        {
            return new ColumnRefNode { TableOrAlias = parts[0], Column = parts[1] };
        }

        throw new ParserException($"Parser Error: Invalid {sideLabel} column reference '{token.Value}'. Expected 'column' or 'table.column'.");
    }

    private List<SqlNode> ParseColumnList()
    {
        var columns = new List<SqlNode>();

        while (!IsEof())
        {
            // We just Peek "FROM", avoiding greedy consumption.
            if (ParserSyntaxHelper.IsKeyword(Current, SqlKeywords.FROM))
            {
                break;
            }

            if (Current.Type == TokenType.Punctuation && Current.Value == SqlPunctuation.StarToken)
            {
                var colNode = new SelectColumnNode { Expression = SqlPunctuation.StarToken };
                Advance();

                if (Match(TokenType.Keyword, SqlKeywords.AS))
                {
                    var aliasToken = Consume(TokenType.Identifier, "column alias");
                    colNode.Alias = aliasToken.Value;
                }

                columns.Add(colNode);
            }
            else if (Current.Type == TokenType.Identifier)
            {
                var identifier = Advance();
                var colNode = new SelectColumnNode();

                if (identifier.Value.EndsWith(".", StringComparison.Ordinal) &&
                    Current.Type == TokenType.Punctuation &&
                    Current.Value == SqlPunctuation.StarToken)
                {
                    Advance(); // consume '*'
                    colNode.Expression = $"{identifier.Value}{SqlPunctuation.StarToken}";
                }
                else
                {
                    colNode.Expression = identifier.Value;
                }

                if (Match(TokenType.Keyword, SqlKeywords.AS))
                {
                    var aliasToken = Consume(TokenType.Identifier, "column alias");
                    colNode.Alias = aliasToken.Value;
                }

                columns.Add(colNode);
            }
            else
            {
                // Skip unexpected tokens in column list for now
                Advance();
                continue;
            }

            // Consume comma if present, otherwise assume end of column list
            Match(TokenType.Punctuation, SqlPunctuation.CommaToken);
        }

        return columns;
    }

    // This adapts the existing Shunting-Yard from StatementParser to use the Lexer's Tokens directly
    private ExpressionNode? ParseWhereExpression(Queue<Token> tokens)
    {
        if (tokens.Count == 0) return null;

        Stack<ExpressionNode> values = new();
        Stack<Token> operators = new();

        while (tokens.Count != 0)
        {
            Token token = tokens.Dequeue();

            if (token.Type == TokenType.Punctuation && token.Value == SqlPunctuation.OpenParenToken)
            {
                operators.Push(token);
            }
            else if (token.Type == TokenType.Punctuation && token.Value == SqlPunctuation.CloseParenToken)
            {
                while (operators.Count > 0 && !(operators.Peek().Type == TokenType.Punctuation && operators.Peek().Value == SqlPunctuation.OpenParenToken))
                {
                    EvaluateTopOperator(values, operators);
                }
                operators.Pop(); // Remove the "("
            }
            else if (token.Type == TokenType.Operator)
            {
                while (operators.Count > 0 &&
                      !(operators.Peek().Type == TokenType.Punctuation && operators.Peek().Value == SqlPunctuation.OpenParenToken) &&
                      GetPrecedence(token.Value) <= GetPrecedence(operators.Peek().Value))
                {
                    EvaluateTopOperator(values, operators);
                }
                operators.Push(token);
            }
            else if (token.Type == TokenType.Identifier)
            {
                string columnName = token.Value;
                string? tableOrAlias = null;

                if (tokens.Count > 0 && tokens.Peek().Type == TokenType.Punctuation && tokens.Peek().Value == SqlPunctuation.DotToken)
                {
                    tokens.Dequeue(); // .
                    tableOrAlias = columnName;
                    columnName = tokens.Dequeue().Value;
                }

                values.Push(new ColumnRefNode { TableOrAlias = tableOrAlias, Column = columnName });
            }
            else if (token.Type == TokenType.StringLiteral)
            {
                values.Push(new LiteralNode { Value = token.Value });
            }
            else if (token.Type == TokenType.NumberLiteral)
            {
                object numValue = token.Value;
                if (int.TryParse(token.Value, out int i)) numValue = i;
                else if (long.TryParse(token.Value, out long l)) numValue = l;
                else if (double.TryParse(token.Value, System.Globalization.CultureInfo.InvariantCulture, out double d)) numValue = d;

                values.Push(new LiteralNode { Value = numValue });
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

        return values.Count != 0 ? values.Pop() : new LiteralNode { Value = SqlLiterals.TrueExpression };
    }

    private void EvaluateTopOperator(Stack<ExpressionNode> values, Stack<Token> operators)
    {
        var opToken = operators.Pop();
        string op = opToken.Value.ToUpperInvariant();

        // Shunting-Yard normally pops right then left
        var right = values.Count > 0 ? values.Pop() : null;
        var left = values.Count > 0 ? values.Pop() : null;

        values.Push(new BinaryExpressionNode
        {
            Operator = op,
            Left = left!,
            Right = right!
        });
    }

    private static int GetPrecedence(string op)
    {
        return op.ToUpperInvariant() switch
        {
            Operators.OR => 1,
            Operators.AND => 2,
            Operators.EQUALS or Operators.NOT_EQUALS or Operators.GREATER_THAN or Operators.LESS_THAN or Operators.GREATER_THAN_OR_EQUAL_TO or Operators.LESS_THAN_OR_EQUAL_TO => 3,
            Operators.ADD or Operators.SUBTRACT => 4,
            Operators.MUL or Operators.DIVIDE => 5,
            _ => -1,
        };
    }
}
