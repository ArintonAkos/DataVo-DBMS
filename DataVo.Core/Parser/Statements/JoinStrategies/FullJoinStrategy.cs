using DataVo.Core.Enums;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Statement;
using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.Types;
using DataVo.Core.Utils;

namespace DataVo.Core.Parser.Statements.JoinStrategies;

/// <summary>
/// Implements the FULL OUTER JOIN evaluation strategy.
/// Computes the intersection matching condition values, including all unmapped source and target rows.
/// </summary>
internal class FullJoinStrategy : IJoinStrategy
{
    /// <summary>
    /// Gets the target string representing a Full Join.
    /// </summary>
    public string JoinType => JoinTypes.FULL;

    /// <summary>
    /// Executes the logical full outer join explicitly resolving constraints bidirectionally.
    /// </summary>
    /// <param name="leftRows">The initial logical dataset explicitly collected earlier in the plan.</param>
    /// <param name="condition">The functional bound specifying exact mappings.</param>
    /// <param name="context">The state defining limits perfectly across boundaries.</param>
    /// <returns>A dictionary containing unique hashed outputs structurally mapping joined sequences.</returns>
    /// <exception cref="EvaluationException">Thrown if the required condition is missing.</exception>
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
        bool insertHashAfter = ShouldInsertHashAfter(context.JoinModel, leftTable, rightTable);

        // Track right row keys that have been matched to cleanly append the unmatched ones
        HashSet<long> matchedRightKeys = [];

        HashedTable result = rightTableData.Count >= IJoinStrategy.HashLookupThreshold
            ? ExecuteHashJoin(leftRows, rightTableData, leftTable, leftColumn, rightTable, rightColumn, insertHashAfter, matchedRightKeys, context)
            : ExecuteNestedLoopJoin(leftRows, rightTableData, leftTable, leftColumn, rightTable, rightColumn, insertHashAfter, matchedRightKeys, context);

        ProcessUnmatchedRightRows(leftRows, rightTableData, rightTable, insertHashAfter, matchedRightKeys, result, context);

        return result;
    }

    /// <summary>
    /// Performs an optimized Hash-based lookup naturally establishing matched targets safely.
    /// </summary>
    private static HashedTable ExecuteHashJoin(
        HashedTable leftRows, 
        TableData rightTableData, 
        string leftTable, 
        string leftColumn, 
        string rightTable, 
        string rightColumn, 
        bool insertHashAfter, 
        HashSet<long> matchedRightKeys,
        JoinStrategyContext context)
    {
        HashedTable result = [];
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

                    JoinedRowId hash = JoinStrategyContext.BuildHash(leftRowEntry.Key, rightRecord.RowId, insertHashAfter);
                    JoinedRow joinedRow = JoinStrategyContext.CreateJoinedRow(
                        leftRowEntry.Value,
                        rightTable,
                        rightRecord.ToRow());

                    result.Add(hash, joinedRow);
                }
            }
            else
            {
                // No match -> Left Join unmatched case
                JoinedRowId hash = JoinStrategyContext.BuildHash(leftRowEntry.Key, null, insertHashAfter);
                JoinedRow joinedRow = context.CreateNullRightRow(
                    leftRowEntry.Value,
                    rightTable);

                result.Add(hash, joinedRow);
            }
        }

        return result;
    }

    /// <summary>
    /// Performs a simple structured Nested Loop sequential evaluation resolving unmatched sequences functionally.
    /// </summary>
    private static HashedTable ExecuteNestedLoopJoin(
        HashedTable leftRows, 
        TableData rightTableData, 
        string leftTable, 
        string leftColumn, 
        string rightTable, 
        string rightColumn, 
        bool insertHashAfter, 
        HashSet<long> matchedRightKeys,
        JoinStrategyContext context)
    {
        HashedTable result = [];

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

                JoinedRowId hash = JoinStrategyContext.BuildHash(leftRowEntry.Key, rightTableRow.Key, insertHashAfter);
                JoinedRow joinedRow = JoinStrategyContext.CreateJoinedRow(
                    leftRowEntry.Value,
                    rightTable,
                    rightTableRow.Value.ToRow());

                result.Add(hash, joinedRow);
            }

            if (!matchFound)
            {
                // No match -> Left Join unmatched case
                JoinedRowId hash = JoinStrategyContext.BuildHash(leftRowEntry.Key, null, insertHashAfter);
                JoinedRow joinedRow = context.CreateNullRightRow(
                    leftRowEntry.Value,
                    rightTable);

                result.Add(hash, joinedRow);
            }
        }

        return result;
    }

    /// <summary>
    /// Evaluates unmarked target records explicitly building null pointers logically avoiding mapped entries.
    /// </summary>
    private static void ProcessUnmatchedRightRows(
        HashedTable leftRows, 
        TableData rightTableData, 
        string rightTable, 
        bool insertHashAfter, 
        HashSet<long> matchedRightKeys, 
        HashedTable result,
        JoinStrategyContext context)
    {
        if (matchedRightKeys.Count >= rightTableData.Count)
        {
            return;
        }

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

                JoinedRowId hash = JoinStrategyContext.BuildHash(null, rightTableRow.Key, insertHashAfter);
                result.Add(hash, nullPaddedRow);
            }
        }
    }

    /// <summary>
    /// Determines the structural direction to append memory identifiers logically securely preserving order.
    /// </summary>
    private static bool ShouldInsertHashAfter(JoinModel joinModel, string leftTable, string rightTable)
    {
        var joinTables = joinModel.JoinTableDetails.Values.Select(jtd => jtd.TableName).ToList();
        return joinTables.IndexOf(leftTable) < joinTables.IndexOf(rightTable);
    }

    /// <summary>
    /// Pre-processes the target table into an optimized memory map explicitly matching target values cleanly.
    /// </summary>
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
