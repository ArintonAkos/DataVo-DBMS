using DataVo.Core.Enums;
using DataVo.Core.Models.Statement.Utils;

namespace DataVo.Core.Models.Statement;

public class JoinModel
{
    public class JoinCondition
    {
        public Column LeftColumn { get; set; }
        public Column RightColumn { get; set; }
        public string JoinType { get; set; }

        public JoinCondition(string leftTableName, string leftColumnName, string rightTableName, string rightColumnName, string joinType = JoinTypes.INNER)
        {
            LeftColumn = new(string.Empty, leftTableName, leftColumnName);
            RightColumn = new(string.Empty, rightTableName, rightColumnName);
            JoinType = joinType;
        }

        public JoinCondition(Column leftColumn, Column rightColumn, string joinType = JoinTypes.INNER)
        {
            LeftColumn = leftColumn;
            RightColumn = rightColumn;
            JoinType = joinType;
        }
    }

    public Dictionary<string, TableDetail> JoinTableDetails { get; set; } = [];
    public List<JoinCondition> JoinConditions { get; set; } = [];

}