using DataVo.Core.Models.Statement;
using DataVo.Core.Parser.Types;

namespace DataVo.Core.Parser.Statements.JoinStrategies;

/// <summary>
/// Defines the core contract for implementing various SQL JOIN operations.
/// </summary>
public interface IJoinStrategy
{
    /// <summary>
    /// The threshold limit for optimizing loop iterations via hash lookups. 
    /// Determines when to switch from nested loops to hash-based joins.
    /// </summary>
    public const int HashLookupThreshold = 32;

    /// <summary>
    /// Gets the string identifier representing the specific type of join (e.g., INNER, LEFT, RIGHT).
    /// </summary>
    string JoinType { get; }

    /// <summary>
    /// Executes the logical join operation, combining rows from the left result set with the target table based on the provided condition.
    /// </summary>
    /// <param name="leftRows">The accumulated result set from previous operations.</param>
    /// <param name="condition">The join condition defining how tables relate to each other.</param>
    /// <param name="context">The context managing table data retrieval and row creation.</param>
    /// <returns>A hashed table containing the newly combined rows.</returns>
    HashedTable Execute(
        HashedTable leftRows,
        JoinModel.JoinCondition? condition,
        JoinStrategyContext context);
}