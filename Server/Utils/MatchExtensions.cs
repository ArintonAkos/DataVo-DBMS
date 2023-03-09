using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Server.Utils
{
    internal static class MatchExtensions
    {
        public static Group NthGroup(this Match match, int n)
        {
            return match.Groups.Values.Skip(n).First();
        }
    }
}
