using DataVo.Core.Enums;
using DataVo.Core.Models.Statement.Utils;

namespace DataVo.Core.Models.Statement;

/// <summary>
/// Represents join metadata for a parsed SELECT statement.
/// </summary>
public class JoinModel
{
    /// <summary>
    /// Represents one resolved join condition.
    /// </summary>
    public class JoinCondition
    {
        /// <summary>
        /// Gets or sets the left join column.
        /// </summary>
        public Column LeftColumn { get; set; }

        /// <summary>
        /// Gets or sets the right join column.
        /// </summary>
        public Column RightColumn { get; set; }

        /// <summary>
        /// Gets or sets the join type.
        /// </summary>
        public string JoinType { get; set; }

        /// <summary>
        /// Initializes a join condition from raw table/column names.
        /// </summary>
        /// <param name="leftTableName">Left table name.</param>
        /// <param name="leftColumnName">Left column name.</param>
        /// <param name="rightTableName">Right table name.</param>
        /// <param name="rightColumnName">Right column name.</param>
        /// <param name="joinType">Join type.</param>
        public JoinCondition(string leftTableName, string leftColumnName, string rightTableName, string rightColumnName, string joinType = JoinTypes.INNER)
        {
            LeftColumn = new(string.Empty, leftTableName, leftColumnName);
            RightColumn = new(string.Empty, rightTableName, rightColumnName);
            JoinType = joinType;
        }

        /// <summary>
        /// Initializes a join condition from resolved column objects.
        /// </summary>
        /// <param name="leftColumn">Left column.</param>
        /// <param name="rightColumn">Right column.</param>
        /// <param name="joinType">Join type.</param>
        public JoinCondition(Column leftColumn, Column rightColumn, string joinType = JoinTypes.INNER)
        {
            LeftColumn = leftColumn;
            RightColumn = rightColumn;
            JoinType = joinType;
        }
    }

    /// <summary>
    /// Gets or sets joined tables keyed by table name.
    /// </summary>
    public Dictionary<string, TableDetail> JoinTableDetails { get; set; } = [];

    /// <summary>
    /// Gets or sets join conditions applied in evaluation.
    /// </summary>
    public List<JoinCondition> JoinConditions { get; set; } = [];

}