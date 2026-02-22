using System.Text.RegularExpressions;

namespace DataVo.Core.Parser;

public enum TokenType
{
    Keyword,      // SELECT, FROM, WHERE, INSERT, INTO, VALUES, CREATE, TABLE, DROP, INDEX, ON
    Identifier,   // TableName, ColumnName (e.g., Users, Age)
    StringLiteral,// 'John Doe', '2023-01-01'
    NumberLiteral,// 42, 3.14
    Operator,     // +, -, *, /, =, !=, >, <, >=, <=, AND, OR
    Punctuation,  // (, ), ,, *
    EOF           // End of File / Query
}

public class Token
{
    public TokenType Type { get; }
    public string Value { get; }

    public Token(TokenType type, string value)
    {
        Type = type;
        Value = value;
    }

    public override string ToString() => $"{Type}: '{Value}'";
}

public class Lexer
{
    private readonly string _input;
    private int _position;

    // Explicit list of SQL keywords we care about right now
    private static readonly HashSet<string> Keywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES",
        "CREATE", "TABLE", "DROP", "INDEX", "ON", "SHOW", "DATABASES",
        "TABLES", "DESCRIBE", "DELETE", "UPDATE", "SET", "USE", "GO",
        "DATABASE", "PRIMARY", "KEY", "UNIQUE", "REFERENCES",
        "INT", "FLOAT", "BIT", "DATE", "VARCHAR", "AS", "BY", "GROUP", "ORDER",
        "AND", "OR", "HAVING", "ASC", "DESC", "ALTER", "ADD", "MODIFY" // Can be classified as Keyword or Operator depending on AST usage. We'll emit as Operator for Shunting Yard compatibility.
    };

    // Multi-character logical operators
    private static readonly string[] MultiCharOperators = { ">=", "<=", "!=", "<>" };

    public Lexer(string input)
    {
        // Strip SQL comments before tokenization
        _input = RemoveSqlComments(input);
        _position = 0;
    }

    public List<Token> Tokenize()
    {
        var tokens = new List<Token>();

        while (_position < _input.Length)
        {
            char current = _input[_position];

            if (char.IsWhiteSpace(current) || current == ';')
            {
                // Skip whitespace and semicolons (since Go/newline acts as terminator right now)
                _position++;
                continue;
            }

            if (current == '\'' || current == '"')
            {
                tokens.Add(ReadStringLiteral(current));
                continue;
            }

            if (char.IsDigit(current) || (current == '-' && _position + 1 < _input.Length && char.IsDigit(_input[_position + 1])))
            {
                tokens.Add(ReadNumberLiteral());
                continue;
            }

            if (char.IsLetter(current) || current == '_')
            {
                tokens.Add(ReadIdentifierOrKeyword());
                continue;
            }

            // Check for multi-character operators first (>=, <=, !=)
            if (TryReadMultiCharOperator(out Token? opToken))
            {
                if (opToken != null) tokens.Add(opToken);
                continue;
            }

            // Single character operators and punctuation
            if (IsSingleCharOperator(current))
            {
                tokens.Add(new Token(TokenType.Operator, current.ToString()));
                _position++;
                continue;
            }

            if (IsPunctuation(current))
            {
                // Asterisk can be an operator (math) or punctuation (SELECT *)
                // We will emit it as Punctuation generally, the Parser handles context.
                tokens.Add(new Token(TokenType.Punctuation, current.ToString()));
                _position++;
                continue;
            }

            throw new Exception($"Lexer Error: Unrecognized character '{current}' at position {_position}");
        }

        tokens.Add(new Token(TokenType.EOF, ""));
        return tokens;
    }

    private Token ReadStringLiteral(char quoteChar)
    {
        _position++; // Skip opening quote
        int start = _position;

        while (_position < _input.Length && _input[_position] != quoteChar)
        {
            // Handle escaped quotes (e.g. '') 
            // Realistically we should process them, but for now just advance
            _position++;
        }

        if (_position >= _input.Length)
            throw new Exception("Lexer Error: Unterminated string literal.");

        string value = _input.Substring(start, _position - start);
        _position++; // Skip closing quote

        // Re-wrap in single quotes so NodeValue.Parse natively handles it as String
        return new Token(TokenType.StringLiteral, $"'{value}'");
    }

    private Token ReadNumberLiteral()
    {
        int start = _position;
        bool hasDecimal = false;

        // Advance past potential negative sign
        if (_input[_position] == '-') _position++;

        while (_position < _input.Length && (char.IsDigit(_input[_position]) || _input[_position] == '.'))
        {
            if (_input[_position] == '.')
            {
                if (hasDecimal) throw new Exception("Lexer Error: Malformed number literal with multiple decimals.");
                hasDecimal = true;
            }
            _position++;
        }

        return new Token(TokenType.NumberLiteral, _input.Substring(start, _position - start));
    }

    private Token ReadIdentifierOrKeyword()
    {
        int start = _position;

        // Identifiers can contain letters, digits, underscores, and dots (e.g., Table.Column)
        while (_position < _input.Length && (char.IsLetterOrDigit(_input[_position]) || _input[_position] == '_' || _input[_position] == '.'))
        {
            _position++;
        }

        string value = _input.Substring(start, _position - start);
        string upper = value.ToUpperInvariant();

        if (Keywords.Contains(upper))
        {
            // SQL syntax uses AND / OR as boolean operators. To keep Node tree compatible, emit as Operators
            if (upper == "AND" || upper == "OR")
            {
                return new Token(TokenType.Operator, upper);
            }
            return new Token(TokenType.Keyword, upper); // Standardize keywords to uppercase for easier parsing
        }

        return new Token(TokenType.Identifier, value); // Keep case sensitivity for table/column names
    }

    private bool TryReadMultiCharOperator(out Token? token)
    {
        token = null;
        if (_position + 1 >= _input.Length) return false;

        string potentialOp = _input.Substring(_position, 2);
        if (Array.Exists(MultiCharOperators, op => op == potentialOp))
        {
            token = new Token(TokenType.Operator, potentialOp);
            _position += 2;
            return true;
        }

        return false;
    }

    private bool IsSingleCharOperator(char c)
    {
        return c == '=' || c == '<' || c == '>' || c == '+' || c == '-' || c == '/';
    }

    private bool IsPunctuation(char c)
    {
        return c == '(' || c == ')' || c == ',' || c == '*';
    }

    private static string RemoveSqlComments(string input)
    {
        string pattern = @"(--[^\r\n]*|/\*[\s\S]*?\*/)";
        return Regex.Replace(input, pattern, string.Empty, RegexOptions.Multiline);
    }
}
