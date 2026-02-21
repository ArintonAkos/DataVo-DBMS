using System.Text.RegularExpressions;

namespace DataVo.Core.Utils;

internal static class MatchExtensions
{
    public static Group NthGroup(this Match match, int n) => match.Groups.Values.Skip(n).First();
}