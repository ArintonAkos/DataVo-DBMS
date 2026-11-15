namespace DataVo.Core.Parser.Utils;

/// <summary>
/// A SQL <c>LIKE</c> pattern: <c>%</c> matches any run of characters (including none), <c>_</c> exactly one
/// character, and every other character itself, compared ordinally (case-sensitively). There is no escape
/// character. A compiled pattern recognises the common prefix, suffix and substring shapes and tests them
/// without scanning the pattern per row.
/// </summary>
internal sealed class SqlLikePattern
{
    private enum Shape { Exact, Prefix, Suffix, Contains, General }

    private readonly string _pattern;
    private readonly string _literal;
    private readonly Shape _shape;

    /// <summary>Compiles <paramref name="pattern"/>.</summary>
    public SqlLikePattern(string pattern)
    {
        _pattern = pattern;
        _literal = pattern;
        _shape = Shape.General;
        if (pattern.Contains('_'))
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
