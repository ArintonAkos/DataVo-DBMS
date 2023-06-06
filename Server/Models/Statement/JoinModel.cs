using Server.Models.Statement.Utils;
using Server.Services;
using System.Text.RegularExpressions;

namespace Server.Models.Statement;

public class JoinModel
{
    public class JoinCondition
    {
        public Column LeftColumn { get; set; }
        public Column RightColumn { get; set; }

        public JoinCondition(string leftTableName, string leftColumnName, string rightTableName, string rightColumnName)
        {
            LeftColumn = new(string.Empty, leftTableName, leftColumnName);
            RightColumn = new(string.Empty, rightTableName, rightColumnName);
        }

        public JoinCondition(Column leftColumn, Column rightColumn)
        {
            LeftColumn = leftColumn;
            RightColumn = rightColumn;
        }
    }

    public Dictionary<string, TableDetail> JoinTableDetails { get; set; } = new();
    public List<JoinCondition> JoinConditions { get; set; } = new();

    public static JoinModel FromMatchGroup(Group group, TableService tableService)
    {
        var model = new JoinModel();

        var joinDetails = TableParserService.ParseJoinTablesAndConditions(group.Value);
        
        var joinTableNames = joinDetails.Item1;
        var joinConditions = joinDetails.Item2;

        int i = 0;
        foreach (var joinDetail in joinTableNames)
        {
            var joinedTableName = joinDetail.Key;
            var tableDetail = joinDetail.Value;

            tableService.AddTableDetail(tableDetail);

            model.JoinTableDetails.Add(tableDetail.GetTableNameInUse(), tableDetail);

            var leftSide = tableService.ParseAndFindTableNameByColumn(joinConditions[i].Item1);
            var rightSide = tableService.ParseAndFindTableNameByColumn(joinConditions[i].Item2);

            var condition = new JoinCondition(
                leftSide.Item1,
                leftSide.Item2,
                rightSide.Item1,
                rightSide.Item2
            );

            model.JoinConditions.Add(condition);

            i++;
        }

        return model;
    }

}