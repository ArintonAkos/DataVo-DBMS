using DataVo.Core.StorageEngine;
using DataVo.Core.Enums;
using DataVo.Core.Models.Statement;
using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.Types;
using DataVo.Core.Utils;

namespace DataVo.Core.Parser.Statements.JoinStrategies;

/// <summary>
/// Implements the CROSS JOIN Cartesian product evaluation strategy.
/// Calculates combinations across sets without requiring formal ON conditional predicates.
/// </summary>
internal class CrossJoinStrategy : IJoinStrategy
{
    /// <summary>
    /// Gets the strategy identity type identifier mapping to Cross joins.
    /// </summary>
    public string JoinType => JoinTypes.CROSS;

    /// <summary>
    /// Executes the Cartesian product matching iteration, logically combining every row of the left 
    /// set with every independent row of the supplied target table.
    /// </summary>
    /// <param name="leftRows">The accumulated result entries preceding this specific operation.</param>
    /// <param name="condition">The parameter condition parsing object structures structurally (mostly ignored in cross joins securely).</param>
    /// <param name="context">The contextual limits bridging operations efficiently against actual sets natively.</param>
    /// <returns>A combined dictionary tracking valid row outputs accurately resolving logic dynamically.</returns>
    public HashedTable Execute(HashedTable leftRows, JoinModel.JoinCondition? condition, JoinStrategyContext context)
    {
        string leftTable = condition!.LeftColumn.TableName;
        string rightTable = condition.RightColumn.TableName;

        TableData rightTableData = context.GetTableData(rightTable);

        // Terminate early if handling the edge-case where the engine inherently starts with an empty logical model base table physically.
        if (leftRows.Count == 0 && condition.LeftColumn.ColumnName == "__dummy__")
        {
             return [];
        }

        return PerformCartesianProduct(leftRows, rightTableData, rightTable, context);
    }

    /// <summary>
    /// Iterates across two diverse datasets multiplying rows accurately mapping linearly.
    /// </summary>
    /// <param name="leftRows">The data mapping bounds logically tracking accumulated records.</param>
    /// <param name="rightTableData">The physical arrays loaded from the right-hand table definition.</param>
    /// <param name="rightTable">The target table string identifier for the added right sequence.</param>
    /// <param name="context">The contextual limits bridging operations efficiently.</param>
    /// <returns>A merged dataset representing the cartesian mappings logically mapped.</returns>
    private static HashedTable PerformCartesianProduct(
        HashedTable leftRows, 
        TableData rightTableData, 
        string rightTable, 
        JoinStrategyContext context)
    {
        HashedTable result = [];

        foreach (var leftRowEntry in leftRows)
        {
            foreach (var rightTableRow in rightTableData)
            {
                JoinedRowId hash = JoinStrategyContext.BuildHash(leftRowEntry.Key, rightTableRow.Key);
                
                JoinedRow joinedRow = JoinStrategyContext.CreateJoinedRow(
                    leftRowEntry.Value,
                    rightTable,
                    rightTableRow.Value.ToRow());

                result.Add(hash, joinedRow);
            }
        }

        return result;
    }
}
