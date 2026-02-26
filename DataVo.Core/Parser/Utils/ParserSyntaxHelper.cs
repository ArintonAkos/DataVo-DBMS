using DataVo.Core.Constants;

namespace DataVo.Core.Parser.Utils;

internal static class ParserSyntaxHelper
{
    private static readonly HashSet<string> JoinKeywords = new(StringComparer.OrdinalIgnoreCase)
    {
        SqlKeywords.JOIN,
        SqlKeywords.INNER,
        SqlKeywords.LEFT,
        SqlKeywords.RIGHT,
        SqlKeywords.OUTER,
        SqlKeywords.CROSS,
        SqlKeywords.FULL
    };

    public static bool IsKeyword(Token token, string keyword)
    {
        return token.Type == TokenType.Keyword && token.Value.Equals(keyword, StringComparison.OrdinalIgnoreCase);
    }

    public static bool IsJoinKeyword(Token token)
    {
        return token.Type == TokenType.Keyword && JoinKeywords.Contains(token.Value);
    }

    public static bool IsGroupByAt(IReadOnlyList<Token> tokens, int index)
    {
        return IsKeywordAt(tokens, index, SqlKeywords.GROUP) &&
               IsKeywordAt(tokens, index + 1, SqlKeywords.BY);
    }

    public static bool IsOrderByAt(IReadOnlyList<Token> tokens, int index)
    {
        return IsKeywordAt(tokens, index, SqlKeywords.ORDER) &&
               IsKeywordAt(tokens, index + 1, SqlKeywords.BY);
    }

    private static bool IsKeywordAt(IReadOnlyList<Token> tokens, int index, string keyword)
    {
        if (index < 0 || index >= tokens.Count)
        {
            return false;
        }

        return IsKeyword(tokens[index], keyword);
    }
}
