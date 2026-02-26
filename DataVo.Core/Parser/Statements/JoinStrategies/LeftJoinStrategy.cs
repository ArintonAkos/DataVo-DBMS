using DataVo.Core.Enums;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Statement;
using DataVo.Core.Parser.Types;
using DataVo.Core.Utils;

namespace DataVo.Core.Parser.Statements.JoinStrategies;

internal class LeftJoinStrategy : IJoinStrategy
{
    public string JoinType => JoinTypes.LEFT;

    public HashedTable Execute(HashedTable leftRows, JoinModel.JoinCondition? condition, JoinStrategyContext context)
    {
        if (condition == null)
        {
            throw new EvaluationException("JOIN execution error: LEFT JOIN requires a condition.");
        }

        string leftTable = condition.LeftColumn.TableName;
        string leftColumn = condition.LeftColumn.ColumnName;
        string rightTable = condition.RightColumn.TableName;
        string rightColumn = condition.RightColumn.ColumnName;

        Dictionary<string, Dictionary<string, dynamic>> rightTableData = context.GetTableData(rightTable);
        HashedTable result = [];
        bool insertHashAfter = ShouldInsertHashAfter(context.JoinModel, leftTable, rightTable);

        if (rightTableData.Count >= IJoinStrategy.HashLookupThreshold)
        {
            var rightLookup = BuildRightLookup(rightTableData, rightColumn);

            foreach (var leftRowEntry in leftRows)
            {
                if (!leftRowEntry.Value.ContainsKey(leftTable) || !leftRowEntry.Value[leftTable].ContainsKey(leftColumn))
                {
                    throw new EvaluationException($"JOIN execution error: left row is missing column '{leftTable}.{leftColumn}'.");
                }

                var leftValue = leftRowEntry.Value[leftTable][leftColumn];

                if (rightLookup.TryGetValue(leftValue, out KeyValuePair<string, Dictionary<string, dynamic>> rightTableRow))
                {
                    string hash = context.BuildHash(leftRowEntry.Key, rightTableRow.Key, insertHashAfter);
                    JoinedRow joinedRow = context.CreateJoinedRow(
                        leftRowEntry.Value,
                        rightTable,
                        rightTableRow.Value.ToRow());

                    result.Add(hash, joinedRow);
                }
                else
                {
                    string hash = context.BuildHash(leftRowEntry.Key, "NULL", insertHashAfter);
                    JoinedRow joinedRow = context.CreateNullRightRow(
                        leftRowEntry.Value,
                        rightTable);

                    result.Add(hash, joinedRow);
                }
            }

            return result;
        }

        // Nested Loop Left Join implementation
        foreach (var leftRowEntry in leftRows)
        {
            if (!leftRowEntry.Value.ContainsKey(leftTable) || !leftRowEntry.Value[leftTable].ContainsKey(leftColumn))
            {
                throw new EvaluationException($"JOIN execution error: left row is missing column '{leftTable}.{leftColumn}'.");
            }

            var leftValue = leftRowEntry.Value[leftTable][leftColumn];
            bool matchFound = false;

            foreach (var rightTableRow in rightTableData)
            {
                if (!rightTableRow.Value.ContainsKey(rightColumn) || rightTableRow.Value[rightColumn] != leftValue)
                {
                    continue;
                }

                matchFound = true;
                string hash = context.BuildHash(leftRowEntry.Key, rightTableRow.Key, insertHashAfter);
                JoinedRow joinedRow = context.CreateJoinedRow(
                    leftRowEntry.Value,
                    rightTable,
                    rightTableRow.Value.ToRow());

                result.Add(hash, joinedRow);
            }

            if (!matchFound)
            {
                string hash = context.BuildHash(leftRowEntry.Key, "NULL", insertHashAfter);
                JoinedRow joinedRow = context.CreateNullRightRow(
                    leftRowEntry.Value,
                    rightTable);

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

    private static Dictionary<dynamic, KeyValuePair<string, Dictionary<string, dynamic>>> BuildRightLookup(
        Dictionary<string, Dictionary<string, dynamic>> rightTableData,
        string rightColumn)
    {
        Dictionary<dynamic, KeyValuePair<string, Dictionary<string, dynamic>>> lookup = [];

        foreach (var rightTableRow in rightTableData)
        {
            if (!rightTableRow.Value.ContainsKey(rightColumn))
            {
                continue;
            }

            dynamic key = rightTableRow.Value[rightColumn];
            if (!lookup.ContainsKey(key))
            {
                lookup[key] = rightTableRow;
            }
        }

        return lookup;
    }
}
