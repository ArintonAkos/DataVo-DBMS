using DataVo.Core.Constants;
using DataVo.Core.Enums;
using DataVo.Core.Exceptions;

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

    public static string ResolveJoinType(IReadOnlyList<string> prefixTokens)
    {
        if (prefixTokens.Count == 0)
        {
            return JoinTypes.INNER;
        }

        if (prefixTokens.Count == 1)
        {
            return prefixTokens[0] switch
            {
                SqlKeywords.INNER => JoinTypes.INNER,
                SqlKeywords.LEFT => JoinTypes.LEFT,
                SqlKeywords.RIGHT => JoinTypes.RIGHT,
                SqlKeywords.FULL => JoinTypes.FULL,
                SqlKeywords.CROSS => JoinTypes.CROSS,
                _ => throw new ParserException($"Parser Error: Unsupported JOIN type '{prefixTokens[0]} JOIN'.")
            };
        }

        if (prefixTokens.Count == 2 && prefixTokens[1] == SqlKeywords.OUTER)
        {
            return prefixTokens[0] switch
            {
                SqlKeywords.LEFT => JoinTypes.LEFT,
                SqlKeywords.RIGHT => JoinTypes.RIGHT,
                SqlKeywords.FULL => JoinTypes.FULL,
                _ => throw new ParserException($"Parser Error: Unsupported JOIN type '{string.Join(" ", prefixTokens)} JOIN'.")
            };
        }

        throw new ParserException($"Parser Error: Unsupported JOIN type '{string.Join(" ", prefixTokens)} JOIN'.");
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
