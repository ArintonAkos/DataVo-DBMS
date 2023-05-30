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

    public static JoinModel FromMatchGroup(Group group, TableService tableService)
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
                
                if (conditionParts.Length != 2)
                {
                    throw new Exception("Invalid join condition");
                }

                var leftSide = tableService.ParseAndFindTableNameByColumn(conditionParts[0]);
                var rightSide = tableService.ParseAndFindTableNameByColumn(conditionParts[1]);

                var condition = new JoinCondition(
                    leftSide.Item1,
                    leftSide.Item2, 
                    rightSide.Item1, 
                    rightSide.Item2
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