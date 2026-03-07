using DataVo.Core.Exceptions;
using DataVo.Core.Models.Statement;
using DataVo.Core.Parser.Types;
using DataVo.Core.Enums;
using DataVo.Core.Utils;
using DataVo.Core.Models.Statement.Utils;

namespace DataVo.Core.Parser.Statements.JoinStrategies;

/// <summary>
/// Implements the INNER JOIN evaluation strategy.
/// Computes the intersection of two data sets based on matching condition values.
/// </summary>
internal class InnerJoinStrategy : IJoinStrategy
{
    /// <summary>
    /// Gets the target string representing an Inner Join.
    /// </summary>
    public string JoinType => JoinTypes.INNER;

    /// <summary>
    /// Executes the INNER JOIN matching logic sequentially or optimally via Hash maps.
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
            throw new EvaluationException("JOIN execution error: INNER JOIN requires a condition.");
        }

        string leftTable = condition.LeftColumn.TableName;
        string leftColumn = condition.LeftColumn.ColumnName;
        string rightTable = condition.RightColumn.TableName;
        string rightColumn = condition.RightColumn.ColumnName;

        TableData rightTableData = context.GetTableData(rightTable);
        bool insertHashAfter = ShouldInsertHashAfter(context.JoinModel, leftTable, rightTable);

        if (rightTableData.Count >= IJoinStrategy.HashLookupThreshold)
        {
            return ExecuteHashJoin(leftRows, rightTableData, leftTable, leftColumn, rightTable, rightColumn, insertHashAfter);
        }

        return ExecuteNestedLoopJoin(leftRows, rightTableData, leftTable, leftColumn, rightTable, rightColumn, insertHashAfter);
    }

    /// <summary>
    /// Performs an optimized Hash-based Join lookup avoiding sequential iterations.
    /// </summary>
    private static HashedTable ExecuteHashJoin(
        HashedTable leftRows, 
        TableData rightTableData, 
        string leftTable, 
        string leftColumn, 
        string rightTable, 
        string rightColumn, 
        bool insertHashAfter)
    {
        HashedTable result = [];
        JoinLookupTable rightLookup = BuildRightLookup(rightTableData, rightColumn);

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
        }

        return result;
    }

    /// <summary>
    /// Performs a simple structured Nested Loop sequential evaluation.
    /// </summary>
    private static HashedTable ExecuteNestedLoopJoin(
        HashedTable leftRows, 
        TableData rightTableData, 
        string leftTable, 
        string leftColumn, 
        string rightTable, 
        string rightColumn, 
        bool insertHashAfter)
    {
        HashedTable result = [];

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

                JoinedRowId hash = JoinStrategyContext.BuildHash(leftRowEntry.Key, rightTableRow.Key, insertHashAfter);
                JoinedRow joinedRow = JoinStrategyContext.CreateJoinedRow(
                    leftRowEntry.Value,
                    rightTable,
                    rightTableRow.Value.ToRow());

                result.Add(hash, joinedRow);
            }
        }

        return result;
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
    private static JoinLookupTable BuildRightLookup(TableData rightTableData, string rightColumn)
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
