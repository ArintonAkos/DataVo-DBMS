using System.Text.RegularExpressions;
using Server.Models.Statement.Utils;
using Server.Parser.Statements;

namespace Server.Models.DML;

internal class DeleteFromModel
{
    public string TableName { get; set; }
    public Where WhereStatement { get; set; }

    public static DeleteFromModel FromMatch(Match match)
    {
        string tableName = match.Groups["TableName"].Value;
        var whereStatement = new Where(match.Groups["WhereStatement"].Value);

        return new DeleteFromModel()
        {
            TableName = tableName,
            WhereStatement = whereStatement,
        };
    }
}