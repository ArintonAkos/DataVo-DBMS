using Server.Models.Statement.Utils;
using Server.Services;
using System;
using System.IO;
using System.Text.RegularExpressions;

namespace Server.Models.Statement;

public class JoinModel
{
    public class JoinCondition
    {
        public string LeftTableName { get; set; }
        public string LeftColumnName { get; set; }
        public string RightTableName { get; set; }
        public string RightColumnName { get; set; }

        public JoinCondition(string leftTableName, string leftColumnName, string rightTableName, string rightColumnName)
        {
            LeftTableName = leftTableName;
            LeftColumnName = leftColumnName;
            RightTableName = rightTableName;
            RightColumnName = rightColumnName;
        }
    }

    public Dictionary<string, TableDetail> JoinTableDetails { get; set; } = new();
    public List<JoinCondition> JoinConditions { get; set; } = new();

    public static JoinModel FromMatchGroup(Group group)
    {
        var model = new JoinModel();

        var joinDetails = TableParserService.ParseJoinTablesAndConditions(
            group.Captures.Cast<Capture>().Select(c => c.Value),
            group.Captures.Cast<Capture>().Select(c => c.Value)
        );

        var joinTableNames = joinDetails.Item1;
        var joinConditionsRaw = joinDetails.Item2;

        int i = 0;
        foreach (var joinDetail in joinTableNames)
        {
            var joinedTableName = joinDetail.Key;
            var tableDetail = joinDetail.Value;

            if (!joinTableNames.ContainsKey(joinedTableName))
            {
                model.JoinTableDetails.Add(tableDetail.GetTableNameInUse(), tableDetail);

                var conditionParts = joinConditionsRaw[i].Split('=');
                var leftTableColumn = conditionParts[0].Trim().Split('.');
                var rightTableColumn = conditionParts[1].Trim().Split('.');

                var condition = new JoinCondition(
                    leftTableColumn[0],
                    leftTableColumn[1],
                    rightTableColumn[0],
                    rightTableColumn[1]
                );

                model.JoinConditions.Add(condition);
            } 
            else
            {
                throw new Exception($"Table '{joinedTableName}' is already in use!");
            }
        }

        return model;
    }

}