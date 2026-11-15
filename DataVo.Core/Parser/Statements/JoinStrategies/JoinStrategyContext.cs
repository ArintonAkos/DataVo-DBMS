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
        if (TableRowsByName is { } narrowed && narrowed.TryGetValue(tableDetail.TableName, out TableData? rows))
        {
            return rows;
        }

        if (tableDetail.TableContent == null)
        {
            throw new EvaluationException($"Join Strategy Error: table '{tableAliasOrName}' has no content loaded.");
        }

        return tableDetail.TableContent;
    }

    /// <summary>
    /// Gets or sets the rows a WHERE filter left for some tables, keyed by table name, while a join runs; a
    /// table without an entry is read whole.
    /// </summary>
    public IReadOnlyDictionary<string, TableData>? TableRowsByName { get; set; }

    /// <summary>
    /// Gets or sets how many tables the left-side rows already span when the current join step runs.
    /// <see cref="Join"/> sets it before every step so unmatched right rows get a key of the right width.
    /// </summary>
    public int LeftTableCount { get; set; } = 1;

    /// <summary>
    /// Builds the key of a joined row by appending the right row's key to the left row's key, so a key always
    /// lists row keys in the order the tables were joined. <see cref="Join"/> later rewrites keys into one
    /// canonical table order, which makes keys comparable whichever table the evaluation started from.
    /// </summary>
    /// <param name="leftRowKey">The key of the accumulated left row.</param>
    /// <param name="rightRowKey">The right row's key, or <see langword="null"/> when an outer join found no match.</param>
    /// <returns>The key of the joined row.</returns>
    public static JoinedRowId BuildHash(JoinedRowId leftRowKey, RowKey? rightRowKey)
    {
        // long.MinValue marks the missing side of an outer join, as before row keys.
        return leftRowKey.Append(rightRowKey ?? long.MinValue);
    }

    /// <summary>
    /// Builds the key of a right row that matched no left row (RIGHT/FULL join): one missing-side marker per
    /// left table followed by the right row's key.
    /// </summary>
    /// <param name="rightRowKey">The unmatched right row's key.</param>
    /// <returns>The key of the null-padded joined row.</returns>
    public JoinedRowId BuildUnmatchedRightHash(RowKey rightRowKey)
    {
        var ids = new List<RowKey>(LeftTableCount + 1);
        for (int i = 0; i < LeftTableCount; i++)
        {
            ids.Add(long.MinValue);
        }

        ids.Add(rightRowKey);
        return new JoinedRowId(ids);
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