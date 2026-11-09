namespace DataVo.Core.Parser.Utils;

/// <summary>Works with string literals as the lexer hands them over: surrounded by single quotes, with escapes already resolved.</summary>
internal static class SqlLiteralText
{
    /// <summary>Removes one surrounding pair of single quotes, leaving any quotes inside as data.</summary>
    /// <param name="text">The literal text.</param>
    /// <returns>The value without its delimiters; unquoted text is returned unchanged.</returns>
    /// <example>
    /// <code>
    /// string name = SqlLiteralText.Unquote("'O'Brien'"); // O'Brien
    /// </code>
    /// </example>
    public static string Unquote(string text) =>
        text.Length >= 2 && text[0] == '\'' && text[^1] == '\'' ? text[1..^1] : text;
}
