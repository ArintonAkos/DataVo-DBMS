using DataVo.Core.Parser.Utils;

namespace DataVo.Core;

/// <summary>
/// Helpers for SQL <c>LIKE</c> patterns. DataVo's <c>LIKE</c> escape character is the backslash (as in PostgreSQL and
/// MySQL): <c>\%</c>, <c>\_</c> and <c>\\</c> match a literal <c>%</c>, <c>_</c> and backslash.
/// </summary>
/// <example>
/// <code>
/// string pattern = DataVoLikePattern.Escape(userText) + "%";   // starts-with, whatever userText holds
/// cmd.Parameters.AddWithValue("@p", pattern);                  // ... WHERE Name LIKE @p
/// </code>
/// </example>
public static class DataVoLikePattern
{
    /// <summary>The escape character of DataVo <c>LIKE</c> patterns.</summary>
    public const char EscapeCharacter = SqlLikePattern.Escape;

    /// <summary>
    /// Escapes <paramref name="text"/> so a <c>LIKE</c> pattern matches it literally: a backslash goes before every
    /// backslash, <c>%</c> and <c>_</c>.
    /// </summary>
    /// <param name="text">The literal text.</param>
    /// <returns>The escaped pattern fragment: <c>50%_off</c> becomes <c>50\%\_off</c>.</returns>
    public static string Escape(string text)
    {
        return SqlLikePattern.EscapeLiteral(text ?? throw new ArgumentNullException(nameof(text)));
    }
}
