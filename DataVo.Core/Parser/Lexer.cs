using System.Text.RegularExpressions;
using DataVo.Core.Exceptions;
using DataVo.Core.Enums;
using DataVo.Core.Constants;

namespace DataVo.Core.Parser;

/// <summary>
/// Represents the kinds of tokens emitted by the lexer.
/// </summary>
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

/// <summary>
/// Represents a single lexical token emitted from SQL source text.
/// </summary>
public class Token
{
    /// <summary>
    /// Gets the token category.
    /// </summary>
    public TokenType Type { get; }

    /// <summary>
    /// Gets the original token value.
    /// </summary>
    public string Value { get; }

    /// <summary>
    /// Initializes a new token instance.
    /// </summary>
    /// <param name="type">The token category.</param>
    /// <param name="value">The token text.</param>
    public Token(TokenType type, string value)
    {
        Type = type;
        Value = value;
    }

    /// <summary>
    /// Returns a debugger-friendly token representation.
    /// </summary>
    public override string ToString() => $"{Type}: '{Value}'";
}

/// <summary>
/// Tokenizes raw SQL text into a flat token stream consumed by the parser.
/// </summary>
/// <example>
/// <code>
/// var lexer = new Lexer("SELECT Id, Name FROM Users WHERE Id = 1");
/// List&lt;Token&gt; tokens = lexer.Tokenize();
/// </code>
/// </example>
public class Lexer
{
    private readonly string _input;
    private int _position;

    /// <summary>
    /// Gets the set of keywords recognized directly by tokenization.
    /// </summary>
    private static readonly HashSet<string> Keywords =
        new([.. SqlKeywords.All, Operators.AND, Operators.OR], StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Gets the supported multi-character operators that should be matched before single-character operators.
    /// </summary>
    private static readonly string[] MultiCharOperators = { Operators.GREATER_THAN_OR_EQUAL_TO, Operators.LESS_THAN_OR_EQUAL_TO, Operators.NOT_EQUALS, "<>" };

    /// <summary>
    /// Initializes a new lexer for SQL input.
    /// </summary>
    /// <param name="input">The SQL text to tokenize.</param>
    public Lexer(string input)
    {
        _input = RemoveSqlComments(input);
        _position = 0;
    }

    /// <summary>
    /// Converts the SQL input into a token list terminated by an EOF token.
    /// </summary>
    /// <returns>The token stream for the current input.</returns>
    public List<Token> Tokenize()
    {
        var tokens = new List<Token>();

        while (_position < _input.Length)
        {
            char current = _input[_position];

            if (char.IsWhiteSpace(current) || current == ';')
            {
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

            if (TryReadMultiCharOperator(out Token? opToken))
            {
                if (opToken != null) tokens.Add(opToken);
                continue;
            }

            if (IsSingleCharOperator(current))
            {
                tokens.Add(new Token(TokenType.Operator, current.ToString()));
                _position++;
                continue;
            }

            if (IsPunctuation(current))
            {
                tokens.Add(new Token(TokenType.Punctuation, current.ToString()));
                _position++;
                continue;
            }

            throw new LexerException($"Lexer Error: Unrecognized character '{current}' at position {_position}");
        }

        tokens.Add(new Token(TokenType.EOF, ""));
        return tokens;
    }

    /// <summary>
    /// Reads a quoted string literal token.
    /// </summary>
    private Token ReadStringLiteral(char quoteChar)
    {
        _position++;
        int start = _position;

        while (_position < _input.Length && _input[_position] != quoteChar)
        {
            _position++;
        }

        if (_position >= _input.Length)
            throw new LexerException("Lexer Error: Unterminated string literal.");

        string value = _input.Substring(start, _position - start);
        _position++;

        return new Token(TokenType.StringLiteral, $"'{value}'");
    }

    /// <summary>
    /// Reads an integer or floating-point number literal.
    /// </summary>
    private Token ReadNumberLiteral()
    {
        int start = _position;
        bool hasDecimal = false;

        if (_input[_position] == '-') _position++;

        while (_position < _input.Length && (char.IsDigit(_input[_position]) || _input[_position] == '.'))
        {
            if (_input[_position] == '.')
            {
                if (hasDecimal) throw new LexerException("Lexer Error: Malformed number literal with multiple decimals.");
                hasDecimal = true;
            }
            _position++;
        }

        return new Token(TokenType.NumberLiteral, _input.Substring(start, _position - start));
    }

    /// <summary>
    /// Reads either an identifier token or a recognized keyword token.
    /// </summary>
    private Token ReadIdentifierOrKeyword()
    {
        int start = _position;

        while (_position < _input.Length && (char.IsLetterOrDigit(_input[_position]) || _input[_position] == '_' || _input[_position] == '.'))
        {
            _position++;
        }

        string value = _input.Substring(start, _position - start);
        string upper = value.ToUpperInvariant();

        if (Keywords.Contains(upper))
        {
            if (upper == Operators.AND || upper == Operators.OR)
            {
                return new Token(TokenType.Operator, upper);
            }
            return new Token(TokenType.Keyword, upper);
        }

        return new Token(TokenType.Identifier, value);
    }

    /// <summary>
    /// Attempts to read a multi-character operator at the current position.
    /// </summary>
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

    /// <summary>
    /// Determines whether the character represents a supported single-character operator.
    /// </summary>
    private bool IsSingleCharOperator(char c)
    {
        return c == Operators.EQUALS[0]
            || c == Operators.LESS_THAN[0]
            || c == Operators.GREATER_THAN[0]
            || c == Operators.ADD[0]
            || c == Operators.SUBTRACT[0]
            || c == Operators.DIVIDE[0];
    }

    /// <summary>
    /// Determines whether the character represents SQL punctuation.
    /// </summary>
    private bool IsPunctuation(char c)
    {
        return c == SqlPunctuation.OpenParen
            || c == SqlPunctuation.CloseParen
            || c == SqlPunctuation.Comma
            || c == SqlPunctuation.Star;
    }

    /// <summary>
    /// Removes single-line and block SQL comments before tokenization.
    /// </summary>
    private static string RemoveSqlComments(string input)
    {
        string pattern = @"(--[^\r\n]*|/\*[\s\S]*?\*/)";
        return Regex.Replace(input, pattern, string.Empty, RegexOptions.Multiline);
    }
}
