using Server.Models.Catalog;
using Server.Utils;
using System.Text.RegularExpressions;

namespace Server.Models.DML
{
    internal class InsertIntoModel
    {
        public String TableName { get; set; }
        public List<List<string>> Rows { get; set; }
        public List<string> Columns { get; set; }

        public static InsertIntoModel FromMatch(Match match)
        {
            string columnsRaw = match.Groups["Columns"].Value.RemoveWhiteSpaces();
            string valuesRaw = match.Groups["Values"].Value.RemoveWhiteSpaces();

            var columns = columnsRaw
                .Split(",")
                .ToList();
            var rows = match.Groups["Values"].Captures
                .Select(v => v.Value.RemoveWhiteSpaces().Split(",").ToList())
                .ToList();

            return new InsertIntoModel
            { 
                TableName = match.Groups["TableName"].Value,
                Rows = rows,
                Columns = columns 
            };
        }

        public void ValidateInputData()
        {
            if (Rows.Any(r => r.Count != Columns.Count))
            {
                throw new Exception("The number of values provided in a row must be the same as " +
                    "the number of columns provided inside the paranthesis after the table name attribute.");
            }
        }
    }
}
