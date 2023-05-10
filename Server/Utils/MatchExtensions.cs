using System.Text.RegularExpressions;

namespace Server.Utils;

internal static class MatchExtensions
{
    public static Group NthGroup(this Match match, int n) => match.Groups.Values.Skip(n).First();
}