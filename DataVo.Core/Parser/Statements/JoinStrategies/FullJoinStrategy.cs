using DataVo.Core.Enums;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Statement;
using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.Types;
using DataVo.Core.Utils;
using System.Collections.Generic;
using System.Linq;

namespace DataVo.Core.Parser.Statements.JoinStrategies;

internal class FullJoinStrategy : IJoinStrategy
{
    public string JoinType => JoinTypes.FULL;

    public HashedTable Execute(HashedTable leftRows, JoinModel.JoinCondition? condition, JoinStrategyContext context)
    {
        if (condition == null)
        {
            throw new EvaluationException("JOIN execution error: FULL JOIN requires a condition.");
        }

        string leftTable = condition.LeftColumn.TableName;
        string leftColumn = condition.LeftColumn.ColumnName;
        string rightTable = condition.RightColumn.TableName;
        string rightColumn = condition.RightColumn.ColumnName;

        TableData rightTableData = context.GetTableData(rightTable);
        HashedTable result = [];
        bool insertHashAfter = ShouldInsertHashAfter(context.JoinModel, leftTable, rightTable);

        // Track right row keys that have been matched to cleanly append the unmatched ones
        HashSet<long> matchedRightKeys = [];

        if (rightTableData.Count >= IJoinStrategy.HashLookupThreshold)
        {
            JoinLookupTable rightLookup = BuildRightGroupedLookup(rightTableData, rightColumn);

            foreach (var leftRowEntry in leftRows)
            {
                if (!leftRowEntry.Value.ContainsKey(leftTable) || !leftRowEntry.Value[leftTable].ContainsKey(leftColumn))
                {
                    throw new EvaluationException($"JOIN execution error: left row is missing column '{leftTable}.{leftColumn}'.");
                }

                var leftValue = leftRowEntry.Value[leftTable][leftColumn];

                if (rightLookup.TryGetValue(leftValue, out List<Record>? rightTableRecords) && rightTableRecords != null)
                {
                    foreach (var rightRecord in rightTableRecords!)
                    {
                        matchedRightKeys.Add(rightRecord.RowId);

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
                    // No match -> Left Join unmatched case
                    JoinedRowId hash = context.BuildHash(leftRowEntry.Key, null, insertHashAfter);
                    JoinedRow joinedRow = context.CreateNullRightRow(
                        leftRowEntry.Value,
                        rightTable);

                    result.Add(hash, joinedRow);
                }
            }
        }
        else
        {
            // Nested Loop Full Join implementation
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
                    matchedRightKeys.Add(rightTableRow.Key);

                    JoinedRowId hash = context.BuildHash(leftRowEntry.Key, rightTableRow.Key, insertHashAfter);
                    JoinedRow joinedRow = context.CreateJoinedRow(
                        leftRowEntry.Value,
                        rightTable,
                        rightTableRow.Value.ToRow());

                    result.Add(hash, joinedRow);
                }

                if (!matchFound)
                {
                    // No match -> Left Join unmatched case
                    JoinedRowId hash = context.BuildHash(leftRowEntry.Key, null, insertHashAfter);
                    JoinedRow joinedRow = context.CreateNullRightRow(
                        leftRowEntry.Value,
                        rightTable);

                    result.Add(hash, joinedRow);
                }
            }
        }

        // Processing unmatched right rows -> Right Join unmatched case
        if (matchedRightKeys.Count < rightTableData.Count)
        {
            JoinedRow? existingLeftRow = leftRows.FirstOrDefault().Value;

            foreach (var rightTableRow in rightTableData)
            {
                if (!matchedRightKeys.Contains(rightTableRow.Key))
                {
                    var dict = new Dictionary<string, Row>();

                    if (existingLeftRow != null)
                    {
                        foreach (var key in existingLeftRow.Keys)
                        {
                            dict[key] = context.TableService.GetNullRowForTable(key);
                        }
                    }
                    else
                    {
                        foreach (var joinTableDetail in context.JoinModel.JoinTableDetails.Values)
                        {
                            if (joinTableDetail.TableName != rightTable)
                            {
                                dict[joinTableDetail.TableName] = context.TableService.GetNullRowForTable(joinTableDetail.TableName);
                            }
                        }
                    }

                    dict[rightTable] = rightTableRow.Value.ToRow();
                    JoinedRow nullPaddedRow = new(dict);

                    JoinedRowId hash = context.BuildHash(null, rightTableRow.Key, insertHashAfter);
                    result.Add(hash, nullPaddedRow);
                }
            }
        }

        return result;
    }

    private static bool ShouldInsertHashAfter(JoinModel joinModel, string leftTable, string rightTable)
    {
        var joinTables = joinModel.JoinTableDetails.Values.Select(jtd => jtd.TableName).ToList();
        return joinTables.IndexOf(leftTable) < joinTables.IndexOf(rightTable);
    }

    private static JoinLookupTable BuildRightGroupedLookup(
        TableData rightTableData,
        string rightColumn)
    {
        JoinLookupTable lookup = [];

        foreach (var rightRowEntry in rightTableData)
        {
            if (!rightRowEntry.Value.ContainsKey(rightColumn))
            {
                continue;
            }

            dynamic key = rightRowEntry.Value[rightColumn];
            lookup.AddRecord(key, rightRowEntry.Value);
        }

        return lookup;
    }
}
