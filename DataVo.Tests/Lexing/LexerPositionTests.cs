using DataVo.Core.Parser;

namespace DataVo.Tests.Lexing;

/// <summary>
/// Token line/column numbers (used by error messages) after making their computation incremental:
/// the lexer no longer rescans the input from the start for every token.
/// </summary>
public sealed class LexerPositionTests
{
    [Fact]
    public void Tokens_CarryTheirLineAndColumn_AcrossLines()
    {
        List<Token> tokens = new Lexer("SELECT a,\n  b >= 2\nFROM t").Tokenize();

        Assert.Equal((1, 1), Position(tokens, "SELECT"));
        Assert.Equal((1, 8), Position(tokens, "a"));
        Assert.Equal((1, 9), Position(tokens, ","));
        Assert.Equal((2, 3), Position(tokens, "b"));
        Assert.Equal((2, 5), Position(tokens, ">="));
        Assert.Equal((2, 8), Position(tokens, "2"));
        Assert.Equal((3, 1), Position(tokens, "FROM"));
        Assert.Equal((3, 6), Position(tokens, "t"));
    }

    [Fact]
    public void LongMultiRowStatement_KeepsPositionsExact()
    {
        var sql = new System.Text.StringBuilder("INSERT INTO t (a) VALUES\n");
        for (int i = 0; i < 2_000; i++)
        {
            sql.Append(i == 0 ? "" : ",\n").Append($"({i})");
        }

        List<Token> tokens = new Lexer(sql.ToString()).Tokenize();
        Token last = tokens.Last(token => token.Value == "1999");
        Assert.Equal(2_001, last.Line); // header line + 2,000 value lines
        Assert.Equal(2, last.Column);
    }

    private static (int Line, int Column) Position(List<Token> tokens, string value)
    {
        Token token = tokens.First(t => t.Value == value);
        return (token.Line, token.Column);
    }
}
