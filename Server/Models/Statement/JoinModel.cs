using System.Text.RegularExpressions;

namespace Server.Models.Statement;

internal class JoinModel
{
    public List<string> JoinTables { get; set; } = new();
    public List<string?> JoinTableAliases { get; set; } = new();
    public List<string> JoinConditions { get; set; } = new();

    public static JoinModel FromMatch(Match match)
    {
        var joinTables = match.Groups["JoinTable"].Captures;
        var joinConditions = match.Groups["JoinCondition"].Captures;

        var model = new JoinModel();

        for (int i = 0; i < joinTables.Count; i++)
        {
            string joinTable = joinTables[i].Value;
            string? joinTableAlias = null;

            if (joinTable.ToLower().Contains("as"))
            {
                var splitJoinTable = joinTable
                    .Split("as")
                    .Select(r => r.Replace(" ", ""))
                    .ToList();

                joinTable = splitJoinTable[index: 0];
                joinTableAlias = splitJoinTable[index: 1];
            }

            string joinCondition = joinConditions[i].Value;

            model.JoinTables.Add(joinTable);
            model.JoinTableAliases.Add(joinTableAlias);
            model.JoinConditions.Add(joinCondition);
        }

        return model;
    }
}