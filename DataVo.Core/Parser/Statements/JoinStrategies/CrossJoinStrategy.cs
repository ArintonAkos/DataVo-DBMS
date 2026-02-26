using DataVo.Core.Enums;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Statement;
using DataVo.Core.Parser.Types;
using DataVo.Core.Utils;
using System.Collections.Generic;
using System.Linq;

namespace DataVo.Core.Parser.Statements.JoinStrategies;

internal class CrossJoinStrategy : IJoinStrategy
{
    public string JoinType => JoinTypes.CROSS;

    public HashedTable Execute(HashedTable leftRows, JoinModel.JoinCondition? condition, JoinStrategyContext context)
    {
        string leftTable = condition!.LeftColumn.TableName;
        string rightTable = condition.RightColumn.TableName;

        Dictionary<string, Dictionary<string, dynamic>> rightTableData = context.GetTableData(rightTable);
        HashedTable result = [];

        bool insertHashAfter = ShouldInsertHashAfter(context.JoinModel, leftTable, rightTable);

        // If leftRows is empty but we are cross joining, we can't Cartesian product with 0 rows
        if (leftRows.Count == 0 && condition.LeftColumn.ColumnName == "__dummy__")
        {
             // DataVo engine edge case for initial cross join if the very first table was physically empty
             return [];
        }

        foreach (var leftRowEntry in leftRows)
        {
            foreach (var rightTableRow in rightTableData)
            {
                string hash = context.BuildHash(leftRowEntry.Key, rightTableRow.Key, insertHashAfter);
                JoinedRow joinedRow = context.CreateJoinedRow(
                    leftRowEntry.Value,
                    rightTable,
                    rightTableRow.Value.ToRow());

                result.Add(hash, joinedRow);
            }
        }

        return result;
    }

    private static bool ShouldInsertHashAfter(JoinModel joinModel, string leftTable, string rightTable)
    {
        List<string> joinTables = joinModel.JoinTableDetails.Values.Select(jtd => jtd.TableName).ToList();
        return joinTables.IndexOf(leftTable) < joinTables.IndexOf(rightTable);
    }
}
