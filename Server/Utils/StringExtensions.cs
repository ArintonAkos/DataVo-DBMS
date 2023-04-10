
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

        public static string TruncateLeftRight(this string source, int charsToTruncate)
        {
            if (source.Length < charsToTruncate)
            {
                return source;
            }

            return source
                .Remove(source.Length - 1, 1)
                .Remove(0, 1);
        }

        public static string Serialize(this string source)
        {
            return source.ToUpper();
        }

        public static bool EqualsSerialized(this string source, string value)
        {
            return source.Serialize().Equals(value.Serialize());
        }
    }
}
