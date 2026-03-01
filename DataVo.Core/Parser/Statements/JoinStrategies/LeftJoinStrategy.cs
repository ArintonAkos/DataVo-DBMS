using DataVo.Core.Enums;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Statement;
using DataVo.Core.Models.Statement.Utils;
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

        TableData rightTableData = context.GetTableData(rightTable);
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

                if (rightLookup.TryGetValue(leftValue, out List<Record>? rightTableRows) && rightTableRows != null)
                {
                    foreach (var rightRecord in rightTableRows!)
                    {
                        JoinedRowId hash = context.BuildHash(leftRowEntry.Key, rightRecord.RowId, insertHashAfter);
                        JoinedRow joinedRow = context.CreateJoinedRow(
                            leftRowEntry.Value,
                            rightTable,
                            rightRecord.ToRow());

                        result.Add(hash, joinedRow);
                    }
                }
                else
                {
                    JoinedRowId hash = context.BuildHash(leftRowEntry.Key, null, insertHashAfter);
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
                JoinedRowId hash = context.BuildHash(leftRowEntry.Key, rightTableRow.Key, insertHashAfter);
                JoinedRow joinedRow = context.CreateJoinedRow(
                    leftRowEntry.Value,
                    rightTable,
                    rightTableRow.Value.ToRow());

                result.Add(hash, joinedRow);
            }

            if (!matchFound)
            {
                JoinedRowId hash = context.BuildHash(leftRowEntry.Key, null, insertHashAfter);
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
        var joinTables = joinModel.JoinTableDetails.Values.Select(jtd => jtd.TableName).ToList();
        return joinTables.IndexOf(leftTable) < joinTables.IndexOf(rightTable);
    }

    private static JoinLookupTable BuildRightLookup(
        TableData rightTableData,
        string rightColumn)
    {
        JoinLookupTable lookup = [];

        foreach (var rightTableRow in rightTableData)
        {
            if (!rightTableRow.Value.ContainsKey(rightColumn))
            {
                continue;
            }

            dynamic key = rightTableRow.Value[rightColumn];
            lookup.AddRecord(key, rightTableRow.Value);
        }

        return lookup;
    }
}
