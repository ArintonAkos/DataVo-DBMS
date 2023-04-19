using Server.Parser.Statements;
using System.Text.RegularExpressions;

namespace Server.Models.DML
{
    internal class DeleteFromModel
    {
        public String TableName { get; set; }
        public Where WhereStatement { get; set; }

        public static DeleteFromModel FromMatch(Match match)
        {
            string tableName = match.Groups["TableName"].Value;
            Where whereStatement = new Where(match.Groups["WhereStatement"].Value);

            return new DeleteFromModel
            {
                TableName = tableName,
                WhereStatement = whereStatement
            };
        }
    }
}
