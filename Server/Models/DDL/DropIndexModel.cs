using System.Text.RegularExpressions;
using Server.Utils;

namespace Server.Models.DDL;

public class DropIndexModel
{
    public DropIndexModel(string indexName, string tableName)
    {
        TableName = tableName;
        IndexName = indexName;
    }

    public string TableName { get; set; }
    public string IndexName { get; set; }

    public static DropIndexModel FromMatch(Match match)
    {
        return new DropIndexModel(match.NthGroup(1).Value, match.NthGroup(2).Value);
    }
}