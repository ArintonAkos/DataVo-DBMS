using DataVo.Core.Enums;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Statement;
using DataVo.Core.Parser.Types;

using DataVo.Core.Utils;
using System.Collections.Generic;
using System.Linq;

namespace DataVo.Core.Parser.Statements.JoinStrategies;

internal class RightJoinStrategy : IJoinStrategy
{
    public string JoinType => JoinTypes.RIGHT;

    public HashedTable Execute(HashedTable leftRows, JoinModel.JoinCondition? condition, JoinStrategyContext context)
    {
        string leftTable = condition!.LeftColumn.TableName;
        string leftColumn = condition.LeftColumn.ColumnName;
        string rightTable = condition.RightColumn.TableName;
        string rightColumn = condition.RightColumn.ColumnName;

        Dictionary<string, Dictionary<string, dynamic>> rightTableData = context.GetTableData(rightTable);
        HashedTable result = [];
        bool insertHashAfter = ShouldInsertHashAfter(context.JoinModel, leftTable, rightTable);

        // Keep track of which right table rows successfully matched at least one left row
        HashSet<string> matchedRightKeys = [];

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
                    matchedRightKeys.Add(rightTableRow.Key);

                    string hash = context.BuildHash(leftRowEntry.Key, rightTableRow.Key, insertHashAfter);
                    JoinedRow joinedRow = context.CreateJoinedRow(
                        leftRowEntry.Value,
                        rightTable,
                        rightTableRow.Value.ToRow());

                    result.Add(hash, joinedRow);
                }
            }
        }
        else
        {
            foreach (var leftRowEntry in leftRows)
            {
                if (!leftRowEntry.Value.ContainsKey(leftTable) || !leftRowEntry.Value[leftTable].ContainsKey(leftColumn))
                {
                    throw new EvaluationException($"JOIN execution error: left row is missing column '{leftTable}.{leftColumn}'.");
                }

                var leftValue = leftRowEntry.Value[leftTable][leftColumn];

                foreach (var rightTableRow in rightTableData)
                {
                    if (!rightTableRow.Value.ContainsKey(rightColumn) || rightTableRow.Value[rightColumn] != leftValue)
                    {
                        continue;
                    }

                    matchedRightKeys.Add(rightTableRow.Key);

                    string hash = context.BuildHash(leftRowEntry.Key, rightTableRow.Key, insertHashAfter);
                    JoinedRow joinedRow = context.CreateJoinedRow(
                        leftRowEntry.Value,
                        rightTable,
                        rightTableRow.Value.ToRow());

                    result.Add(hash, joinedRow);
                }
            }
        }

        // Output all unmatched right rows populated with NULLs for all left tables
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
                        // Fallback if the entire left pipeline was completely empty
                        foreach (var joinTableDetail in context.JoinModel.JoinTableDetails.Values)
                        {
                            if (joinTableDetail.TableName != rightTable)
                            {
                                dict[joinTableDetail.TableName] = context.TableService.GetNullRowForTable(joinTableDetail.TableName);
                            }
                        }
                    }

                    dict[rightTable] = rightTableRow.Value.ToRow();
                    JoinedRow nullPaddedRow = new JoinedRow(dict);

                    string hash = context.BuildHash("NULL", rightTableRow.Key, insertHashAfter);
                    result.Add(hash, nullPaddedRow);
                }
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
