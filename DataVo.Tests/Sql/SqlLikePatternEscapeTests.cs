using System.Text;
using DataVo.Core.Parser.Utils;
using DataVo.Core;

namespace DataVo.Tests.Sql;

/// <summary>
/// The backslash escape of <see cref="SqlLikePattern"/>: escaped wildcards match literally on the compiled shapes,
/// the general matcher, the static matcher and the UTF-8 matcher alike.
/// </summary>
public sealed class SqlLikePatternEscapeTests
{
    [Theory]
    [InlineData(@"50\%", "50%", true)]
    [InlineData(@"50\%", "500", false)]
    [InlineData(@"50\%%", "50%x", true)]
    [InlineData(@"50\%%", "50x", false)]
    [InlineData(@"%\_off", "a_off", true)]
    [InlineData(@"%\_off", "a-off", false)]
    [InlineData(@"%\%%", "1%2", true)]
    [InlineData(@"%\%%", "12", false)]
    [InlineData(@"a\_b_c", "a_bXc", true)]
    [InlineData(@"a\_b_c", "aXbXc", false)]
    [InlineData(@"a%\%b", "aZZ%b", true)]
    [InlineData(@"a%\%b", "aZZb", false)]
    [InlineData(@"C:\\%", @"C:\dir", true)]
    [InlineData(@"C:\\%", "C:dir", false)]
    [InlineData(@"end\", @"end\", true)]
    [InlineData(@"\é_", "éx", true)]
    public void EscapedPatterns_MatchLiterally(string pattern, string input, bool expected)
    {
        var compiled = new SqlLikePattern(pattern);

        Assert.Equal(expected, compiled.IsMatch(input));
        Assert.Equal(expected, SqlLikePattern.IsMatch(input, pattern));
        if (compiled.TryIsMatchUtf8(Encoding.UTF8.GetBytes(input), out bool utf8Match))
        {
            Assert.Equal(expected, utf8Match);
        }
    }

    [Theory]
    [InlineData("plain", "plain")]
    [InlineData("50%_off", @"50\%\_off")]
    [InlineData(@"C:\dir", @"C:\\dir")]
    public void DataVoLikePatternEscape_EscapesEveryMetaCharacter(string text, string expected)
    {
        string escaped = DataVoLikePattern.Escape(text);

        Assert.Equal(expected, escaped);
        Assert.True(new SqlLikePattern(escaped).IsMatch(text));
        Assert.True(new SqlLikePattern("%" + escaped + "%").IsMatch("xx" + text + "yy"));
    }
}
