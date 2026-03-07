using DataVo.Core.Models.Statement;
using DataVo.Core.Exceptions;
using DataVo.Core.Parser.Types;
using DataVo.Core.Services;
using DataVo.Core.Models.Statement.Utils;

namespace DataVo.Core.Parser.Statements.JoinStrategies;

/// <summary>
/// Manages state and provides utility methods for executing table joins.
/// Centralizes data retrieval, hashing, and row instantiation logic.
/// </summary>
public class JoinStrategyContext
{
    /// <summary>
    /// Gets or sets the service maintaining the metadata and instances of active tables.
    /// </summary>
    public TableService TableService { get; set; } = null!;

    /// <summary>
    /// Gets or sets the original JoinModel containing all defined join instructions.
    /// </summary>
    public JoinModel JoinModel { get; set; } = null!;

    /// <summary>
    /// Retrieves the fully loaded logic rows for the specified table alias or name.
    /// </summary>
    /// <param name="tableAliasOrName">The identity pointer string.</param>
    /// <returns>The underlying dataset records wrapped in a Dictionary construct.</returns>
    /// <exception cref="EvaluationException">Thrown if the required table has no content loaded.</exception>
    public TableData GetTableData(string tableAliasOrName)
    {
        var tableDetail = TableService.GetTableDetailByAliasOrName(tableAliasOrName);

        if (tableDetail.TableContent == null)
        {
            throw new EvaluationException($"Join Strategy Error: table '{tableAliasOrName}' has no content loaded.");
        }

        return tableDetail.TableContent;
    }

    /// <summary>
    /// Constructs a compound unique identifier used in join mapping evaluations.
    /// </summary>
    /// <param name="leftRowKey">The hash id defining the base sequence element. Can be null initially.</param>
    /// <param name="rightRowKey">The secondary hash mapping directly to records.</param>
    /// <param name="insertHashAfter">Determines memory injection sequencing.</param>
    /// <returns>A compounded <see cref="JoinedRowId"/> resolving uniqueness limits.</returns>
    public static JoinedRowId BuildHash(JoinedRowId? leftRowKey, long? rightRowKey, bool insertHashAfter)
    {
        long rKey = rightRowKey ?? long.MinValue;
        
        if (leftRowKey == null)
        {
            return insertHashAfter 
                ? new JoinedRowId(long.MinValue, rKey) 
                : new JoinedRowId(rKey, long.MinValue);
        }
        
        return insertHashAfter
            ? leftRowKey.Append(rKey)
            : leftRowKey.Prepend(rKey);
    }

    /// <summary>
    /// Combines data rows from the current evaluation process into a singular wrapped JoinedRow securely mapping bounds.
    /// </summary>
    /// <param name="existingLeftRow">The base row already gathered from previous cycles mapping attributes seamlessly.</param>
    /// <param name="rightTable">The target definition identity formatting references.</param>
    /// <param name="rightRow">The freshly fetched logical sequence attaching properly mapping structs intuitively.</param>
    /// <returns>A new dynamically organized row wrapping logical bounds predictably.</returns>
    public static JoinedRow CreateJoinedRow(JoinedRow existingLeftRow, string rightTable, Row rightRow)
    {
        var dict = new Dictionary<string, Row>();
        
        foreach (var key in existingLeftRow.Keys)
        {
            dict[key] = existingLeftRow[key];
        }
        
        dict[rightTable] = rightRow;
        
        return new JoinedRow(dict);
    }

    /// <summary>
    /// Creates a new JoinedRow by appending an empty (null) representation of the right table's schema.
    /// This is primarily used for OUTER joins when no matching record is found.
    /// </summary>
    /// <param name="existingLeftRow">The base row already gathered from previous cycles.</param>
    /// <param name="rightTable">The name of the target table that failed to produce a match.</param>
    /// <returns>A newly formed JoinedRow with null pointers for the right-hand columns.</returns>
    public JoinedRow CreateNullRightRow(JoinedRow existingLeftRow, string rightTable)
    {
        var dict = new Dictionary<string, Row>();
        
        foreach (var key in existingLeftRow.Keys)
        {
            dict[key] = existingLeftRow[key];
        }
        
        dict[rightTable] = TableService.GetNullRowForTable(rightTable);
        
        return new JoinedRow(dict);
    }
}