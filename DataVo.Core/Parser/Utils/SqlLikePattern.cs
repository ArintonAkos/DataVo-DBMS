namespace DataVo.Core.Parser.Utils;

/// <summary>
/// A SQL <c>LIKE</c> pattern: <c>%</c> matches any run of characters (including none), <c>_</c> exactly one
/// character, and every other character itself, compared ordinally (case-sensitively). A backslash escapes the
/// character after it (<c>\%</c>, <c>\_</c> and <c>\\</c> match a literal <c>%</c>, <c>_</c> and backslash), the default
/// <c>LIKE</c> escape of PostgreSQL and MySQL; a trailing lone backslash matches itself. A compiled pattern
/// recognises the common prefix, suffix and substring shapes and tests them without scanning the pattern per row.
/// <para>
/// A pattern can also be matched against UTF-8 bytes (<see cref="TryIsMatchUtf8"/>), which gives the same
/// answer without decoding the text: a literal run of the pattern can only match whole characters of valid
/// UTF-8, and <c>_</c> is one byte exactly when the text is ASCII.
/// </para>
/// </summary>
internal sealed class SqlLikePattern
{
    private enum Shape { Exact, Prefix, Suffix, Contains, General }

    /// <summary>The escape character: the next pattern character is matched literally.</summary>
    internal const char Escape = '\\';

    private readonly string _pattern;
    private readonly string _literal;
    private readonly Shape _shape;
    private readonly byte[]? _patternUtf8;
    private readonly byte[]? _literalUtf8;
    private readonly bool _hasUnderscore;

    /// <summary>Compiles <paramref name="pattern"/>.</summary>
    public SqlLikePattern(string pattern)
    {
        _pattern = pattern;
        _literal = pattern;
        _shape = Shape.General;
        _patternUtf8 = Utf8Text.TryEncode(pattern);
        _literalUtf8 = _patternUtf8;
        _hasUnderscore = pattern.Contains('_');
        if (pattern.Contains(Escape))
        {
            if (TryGetEscapedShape(pattern, out Shape escapedShape, out string literal))
            {
                _shape = escapedShape;
                _literal = literal;
                _literalUtf8 = _patternUtf8 is null ? null : Utf8Text.TryEncode(literal);
            }

            return;
        }

        if (_hasUnderscore)
        {
            return;
        }

        int leading = 0;
        while (leading < pattern.Length && pattern[leading] == '%')
        {
            leading++;
        }

        int trailing = 0;
        while (trailing < pattern.Length - leading && pattern[pattern.Length - 1 - trailing] == '%')
        {
            trailing++;
        }

        string middle = pattern[leading..(pattern.Length - trailing)];
        if (middle.Contains('%'))
        {
            return;
        }

        _literal = middle;
        _literalUtf8 = _patternUtf8 is null ? null : Utf8Text.TryEncode(middle);
        _shape = (leading > 0, trailing > 0) switch
        {
            (false, false) => Shape.Exact,
            (false, true) => Shape.Prefix,
            (true, false) => Shape.Suffix,
            _ => Shape.Contains,
        };
    }

    /// <summary>Whether <paramref name="input"/> matches the whole pattern.</summary>
    public bool IsMatch(ReadOnlySpan<char> input) => _shape switch
    {
        Shape.Exact => input.SequenceEqual(_literal),
        Shape.Prefix => input.StartsWith(_literal),
        Shape.Suffix => input.EndsWith(_literal),
        Shape.Contains => input.IndexOf(_literal) >= 0,
        _ => IsMatch(input, _pattern),
    };

    /// <summary>
    /// Matches UTF-8 text without decoding it. Returns false when the answer cannot be told from the bytes
    /// (the pattern has a <c>_</c> and the text is not ASCII, or the pattern is not valid Unicode); the caller
    /// then decodes the text and uses <see cref="IsMatch(ReadOnlySpan{char})"/>.
    /// </summary>
    /// <param name="utf8">The text as valid UTF-8.</param>
    /// <param name="isMatch">Whether the text matches the whole pattern, when the method returns true.</param>
    /// <example>
    /// <code>
    /// var pattern = new SqlLikePattern("S0042%");
    /// pattern.TryIsMatchUtf8("S0042-A"u8, out bool isMatch); // true, isMatch = true
    /// </code>
    /// </example>
    public bool TryIsMatchUtf8(ReadOnlySpan<byte> utf8, out bool isMatch)
    {
        isMatch = false;
        if (_patternUtf8 is null || _literalUtf8 is null)
        {
            return false;
        }

        switch (_shape)
        {
            case Shape.Exact:
                isMatch = utf8.SequenceEqual(_literalUtf8);
                return true;
            case Shape.Prefix:
                isMatch = utf8.StartsWith(_literalUtf8);
                return true;
            case Shape.Suffix:
                isMatch = utf8.EndsWith(_literalUtf8);
                return true;
            case Shape.Contains:
                isMatch = utf8.IndexOf(_literalUtf8) >= 0;
                return true;
        }

        // '_' is one character: one byte only in ASCII text.
        if (_hasUnderscore && !Utf8Text.IsAscii(utf8))
        {
            return false;
        }

        isMatch = IsMatchBytes(utf8, _patternUtf8);
        return true;
    }

    /// <summary>
    /// Finds the shape of a pattern holding escapes: leading and trailing unescaped <c>%</c> runs around a middle
    /// with no unescaped wildcard, whose unescaped text becomes the literal.
    /// </summary>
    private static bool TryGetEscapedShape(string pattern, out Shape shape, out string literal)
    {
        shape = Shape.General;
        literal = pattern;
        int leading = 0;
        while (leading < pattern.Length && pattern[leading] == '%')
        {
            leading++;
        }

        var middle = new System.Text.StringBuilder(pattern.Length);
        int trailing = 0;
        for (int i = leading; i < pattern.Length; i++)
        {
            char c = pattern[i];
            if (c == Escape && i + 1 < pattern.Length)
            {
                if (trailing > 0)
                {
                    return false;
                }

                middle.Append(pattern[++i]);
            }
            else if (c == '%')
            {
                trailing++;
            }
            else if (c == '_' || trailing > 0)
            {
                return false;
            }
            else
            {
                middle.Append(c);
            }
        }

        literal = middle.ToString();
        shape = (leading > 0, trailing > 0) switch
        {
            (false, false) => Shape.Exact,
            (false, true) => Shape.Prefix,
            (true, false) => Shape.Suffix,
            _ => Shape.Contains,
        };
        return true;
    }

    /// <summary>
    /// Escapes <paramref name="text"/> so a <c>LIKE</c> pattern matches it literally: backslash, <c>%</c> and <c>_</c>
    /// each get a preceding backslash.
    /// </summary>
    /// <param name="text">The literal text.</param>
    /// <returns>The escaped pattern fragment.</returns>
    public static string EscapeLiteral(string text)
    {
        if (text.AsSpan().IndexOfAny(Escape, '%', '_') < 0)
        {
            return text;
        }

        var escaped = new System.Text.StringBuilder(text.Length + 8);
        foreach (char c in text)
        {
            if (c is Escape or '%' or '_')
            {
                escaped.Append(Escape);
            }

            escaped.Append(c);
        }

        return escaped.ToString();
    }

    /// <summary>The byte form of <see cref="IsMatch(ReadOnlySpan{char}, ReadOnlySpan{char})"/>: the same greedy match over UTF-8.</summary>
    private static bool IsMatchBytes(ReadOnlySpan<byte> input, ReadOnlySpan<byte> pattern)
    {
        int p = 0;
        int s = 0;
        int star = -1;
        int resume = 0;
        while (s < input.Length)
        {
            if (p < pattern.Length && pattern[p] == (byte)'%')
            {
                star = p++;
                resume = s;
            }
            else if (p < pattern.Length && MatchesOne(pattern, p, input[s], out int width))
            {
                p += width;
                s++;
            }
            else if (star >= 0)
            {
                p = star + 1;
                s = ++resume;
            }
            else
            {
                return false;
            }
        }

        while (p < pattern.Length && pattern[p] == (byte)'%')
        {
            p++;
        }

        return p == pattern.Length;
    }

    /// <summary>Whether <paramref name="input"/> matches the whole of <paramref name="pattern"/>, without compiling it.</summary>
    public static bool IsMatch(ReadOnlySpan<char> input, ReadOnlySpan<char> pattern)
    {
        // Greedy match that, on a mismatch, retries from the last '%' one character further on.
        int p = 0;
        int s = 0;
        int star = -1;
        int resume = 0;
        while (s < input.Length)
        {
            if (p < pattern.Length && pattern[p] == '%')
            {
                star = p++;
                resume = s;
            }
            else if (p < pattern.Length && MatchesOne(pattern, p, input[s], out int width))
            {
                p += width;
                s++;
            }
            else if (star >= 0)
            {
                p = star + 1;
                s = ++resume;
            }
            else
            {
                return false;
            }
        }

        while (p < pattern.Length && pattern[p] == '%')
        {
            p++;
        }

        return p == pattern.Length;
    }

    /// <summary>
    /// Matches one input character against the pattern element at <paramref name="p"/>: an escaped character (two
    /// pattern characters), <c>_</c>, or a literal.
    /// </summary>
    private static bool MatchesOne(ReadOnlySpan<char> pattern, int p, char input, out int width)
    {
        char c = pattern[p];
        if (c == Escape && p + 1 < pattern.Length)
        {
            width = 2;
            return pattern[p + 1] == input;
        }

        width = 1;
        return c == '_' || c == input;
    }

    /// <summary>The UTF-8 form of <see cref="MatchesOne(ReadOnlySpan{char}, int, char, out int)"/>; an escaped multi-byte
    /// character matches its remaining bytes as literals.</summary>
    private static bool MatchesOne(ReadOnlySpan<byte> pattern, int p, byte input, out int width)
    {
        byte c = pattern[p];
        if (c == (byte)Escape && p + 1 < pattern.Length)
        {
            width = 2;
            return pattern[p + 1] == input;
        }

        width = 1;
        return c == (byte)'_' || c == input;
    }
}
