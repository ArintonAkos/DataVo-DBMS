
using System.Text.RegularExpressions;

namespace Server.Utils
{
    internal static class StringExtensions
    {
        public static bool ContainsAny(this string haystack, params string[] needles)
        {
            foreach (string needle in needles)
            {
                if (haystack.Contains(needle))
                    return true;
            }

            return false;
        }

        public static string RemoveWhiteSpaces(this string source)
        {
            return Regex.Replace(source, @"\s+", "");
        }

        public static string MatchToParsable(this string source)
        {
            return Regex.Replace(source, @"^\((.*)\),", @"$1");
        }
    }
}
