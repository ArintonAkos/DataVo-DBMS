namespace DataVo.Core.Parser.Utils;

/// <summary>
/// A SQL <c>LIKE</c> pattern: <c>%</c> matches any run of characters (including none), <c>_</c> exactly one
/// character, and every other character itself, compared ordinally (case-sensitively). There is no escape
/// character. A compiled pattern recognises the common prefix, suffix and substring shapes and tests them
/// without scanning the pattern per row.
/// <para>
/// A pattern can also be matched against UTF-8 bytes (<see cref="TryIsMatchUtf8"/>), which gives the same
/// answer without decoding the text: a literal run of the pattern can only match whole characters of valid
/// UTF-8, and <c>_</c> is one byte exactly when the text is ASCII.
/// </para>
/// </summary>
internal sealed class SqlLikePattern
{
    private enum Shape { Exact, Prefix, Suffix, Contains, General }

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
            else if (p < pattern.Length && (pattern[p] == (byte)'_' || pattern[p] == input[s]))
            {
                p++;
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
            else if (p < pattern.Length && (pattern[p] == '_' || pattern[p] == input[s]))
            {
                p++;
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
}
