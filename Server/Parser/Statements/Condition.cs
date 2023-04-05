using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Server.Parser.Statements
{
    internal class Condition
    {
        private string LeftHand { get; set; }
        private string Operand { get; set; }
        private string RightHand { get; set; }

        public static Condition FromMatch(Match match)
        {
            string matchValue = match.Value.Replace("  ", " ").Trim();
            string[] matchParts = matchValue.Split(" ", 3);

            return new Condition
            {
                LeftHand = matchParts[0],
                Operand = matchParts[1],
                RightHand = matchParts[2],
            };
        }

        public Boolean IsValid
        {
            get
            {

            }
        }
    }
}
