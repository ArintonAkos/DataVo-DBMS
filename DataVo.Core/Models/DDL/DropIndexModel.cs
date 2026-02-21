using System.Text.RegularExpressions;
using DataVo.Core.Utils;

namespace DataVo.Core.Models.DDL;

public class DropIndexModel
{
    public DropIndexModel(string indexName, string tableName)
    {
        TableName = tableName;
        IndexName = indexName;
    }

    public string TableName { get; set; }
    public string IndexName { get; set; }

    public static DropIndexModel FromMatch(Match match) => new(match.NthGroup(n: 1).Value, match.NthGroup(n: 2).Value);
}