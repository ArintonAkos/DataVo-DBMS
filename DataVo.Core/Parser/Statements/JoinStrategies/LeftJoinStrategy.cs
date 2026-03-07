using DataVo.Core.Enums;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Statement;
using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.Types;
using DataVo.Core.Utils;

namespace DataVo.Core.Parser.Statements.JoinStrategies;

/// <summary>
/// Implements the LEFT OUTER JOIN evaluation strategy.
/// Computes the intersection matching condition values, including all left rows with nulls when no matches are found natively.
/// </summary>
internal class LeftJoinStrategy : IJoinStrategy
{
    /// <summary>
    /// Gets the target string representing a Left Join.
    /// </summary>
    public string JoinType => JoinTypes.LEFT;

    /// <summary>
    /// Executes the logical outer join logically preserving source records while attaching target fields.
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
            throw new EvaluationException("JOIN execution error: LEFT JOIN requires a condition.");
        }

        string leftTable = condition.LeftColumn.TableName;
        string leftColumn = condition.LeftColumn.ColumnName;
        string rightTable = condition.RightColumn.TableName;
        string rightColumn = condition.RightColumn.ColumnName;

        TableData rightTableData = context.GetTableData(rightTable);
        bool insertHashAfter = ShouldInsertHashAfter(context.JoinModel, leftTable, rightTable);

        if (rightTableData.Count >= IJoinStrategy.HashLookupThreshold)
        {
            return ExecuteHashJoin(leftRows, rightTableData, leftTable, leftColumn, rightTable, rightColumn, insertHashAfter, context);
        }

        return ExecuteNestedLoopJoin(leftRows, rightTableData, leftTable, leftColumn, rightTable, rightColumn, insertHashAfter, context);
    }

    /// <summary>
    /// Performs an optimized Hash-based Left Join lookup handling default outer allocations.
    /// </summary>
    private static HashedTable ExecuteHashJoin(
        HashedTable leftRows, 
        TableData rightTableData, 
        string leftTable, 
        string leftColumn, 
        string rightTable, 
        string rightColumn, 
        bool insertHashAfter, 
        JoinStrategyContext context)
    {
        HashedTable result = [];
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
    /// Performs a sequential Nested Loop Left Join implementation properly establishing mapping limits safely.
    /// </summary>
    private static HashedTable ExecuteNestedLoopJoin(
        HashedTable leftRows, 
        TableData rightTableData, 
        string leftTable, 
        string leftColumn, 
        string rightTable, 
        string rightColumn, 
        bool insertHashAfter, 
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
                JoinedRowId hash = JoinStrategyContext.BuildHash(leftRowEntry.Key, rightTableRow.Key, insertHashAfter);
                JoinedRow joinedRow = JoinStrategyContext.CreateJoinedRow(
                    leftRowEntry.Value,
                    rightTable,
                    rightTableRow.Value.ToRow());

                result.Add(hash, joinedRow);
            }

            if (!matchFound)
            {
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
    /// Determines the correct structural direction to append memory identifiers logically.
    /// </summary>
    private static bool ShouldInsertHashAfter(JoinModel joinModel, string leftTable, string rightTable)
    {
        var joinTables = joinModel.JoinTableDetails.Values.Select(jtd => jtd.TableName).ToList();
        return joinTables.IndexOf(leftTable) < joinTables.IndexOf(rightTable);
    }

    /// <summary>
    /// Pre-processes the target table into an optimized memory map explicitly structurally enhancing lookups.
    /// </summary>
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
