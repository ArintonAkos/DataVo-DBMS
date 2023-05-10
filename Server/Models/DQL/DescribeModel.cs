using System.Text.RegularExpressions;
using Server.Utils;

namespace Server.Models.DQL;

internal class DescribeModel
{
    public DescribeModel(string tableName)
    {
        TableName = tableName;
    }

    public string TableName { get; set; }

    public static DescribeModel FromMatch(Match match)
    {
        return new DescribeModel(match.NthGroup(1).Value);
    }
}