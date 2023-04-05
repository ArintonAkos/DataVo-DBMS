using Server.Parser.Statements;
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
        private Node Statement;

        public static WhereModel FromString(string value)
        {
            Node statement = StatementParser.Parse(value);

            return new WhereModel
            {
                Statement = statement
            };
        }

        public Boolean Evaluate(Dictionary<string, dynamic> dictionary)
        {
            // TO-DO: @Bulcsu - Implement this method
            // I'm not sure what the input parameter type should be,
            // but my suggestions would be to get a dictionary with the
            // column names as keys and the values as values

            return StatementEvaluator.Evaluate(Statement, dictionary);
        }
    }
}
