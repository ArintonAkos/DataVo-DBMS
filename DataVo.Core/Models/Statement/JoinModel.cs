using DataVo.Core.Models.Statement.Utils;

namespace DataVo.Core.Models.Statement;

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

    public Dictionary<string, TableDetail> JoinTableDetails { get; set; } = [];
    public List<JoinCondition> JoinConditions { get; set; } = [];

}