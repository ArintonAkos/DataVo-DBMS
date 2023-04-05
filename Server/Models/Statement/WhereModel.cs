using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Server.Models.Statement
{
    internal class WhereModel
    {
        private Stack<object> Stack;

        public static WhereModel FromMatch(Match match)
        {
            var stack = ParseInputString(match.Value);

            return new WhereModel
            {
                Stack = stack
            };
        }

        private static Stack<object> ParseInputString(string inputString)
        {
            string[] tokens = inputString.Split(' ').Reverse().ToArray();

            foreach (string token in tokens)
            {
                if (token == "AND" || token == "OR")
                {
                    stack.Push(token);
                }
                else
                {
                    stack.Push(token);
                }
            }
        }
    }
}
