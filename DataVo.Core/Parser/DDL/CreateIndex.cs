using System.Text.RegularExpressions;
using DataVo.Core.Logging;
using DataVo.Core.Models.DDL;
using DataVo.Core.Parser.Actions;
using DataVo.Core.BTree;
using DataVo.Core.StorageEngine;
using DataVo.Core.Parser.AST;
using DataVo.Core.Models.Statement.Utils;

namespace DataVo.Core.Parser.DDL;

/// <summary>
/// Handles the <c>CREATE INDEX</c> DDL statement.
/// <para>
/// Registers a new index in the system catalog and populates it with data from
/// all existing rows in the target table. The index is backed by a B-Tree structure
/// managed by <see cref="IndexManager"/>.
/// </para>
/// <para>
/// For composite indexes (multiple columns), the key is formed by concatenating
/// column values with <c>"##"</c> as a delimiter.
/// </para>
/// </summary>
/// <param name="ast">The parsed <see cref="CreateIndexStatement"/> AST node.</param>
/// <example>
/// <code>
/// var ast = new CreateIndexStatement { IndexName = "Idx_LastName", TableName = "Users" };
/// var action = new CreateIndex(ast);
/// action.PerformAction(sessionId);
/// // Output message: "New index file Idx_LastName successfully created!"
/// </code>
/// </example>
internal class CreateIndex(CreateIndexStatement ast) : BaseDbAction
{
    /// <summary>
    /// The parsed model containing the index name, target table, and indexed column names.
    /// </summary>
    private readonly CreateIndexModel _model = CreateIndexModel.FromAst(ast);

    /// <summary>
    /// Executes the index creation pipeline:
    /// <list type="number">
    ///   <item><description>Resolves the active database from the session cache.</description></item>
    ///   <item><description>Registers the index definition in the system catalog.</description></item>
    ///   <item><description>Reads all existing rows from the target table via the storage engine.</description></item>
    ///   <item><description>Builds the index key-to-rowID mapping from the existing data.</description></item>
    ///   <item><description>Creates the B-Tree index file via <see cref="IndexManager"/>.</description></item>
    /// </list>
    /// </summary>
    /// <param name="session">The session identifier used to resolve the active database from the cache.</param>
    /// <remarks>
    /// On failure, the error message is logged and appended to <see cref="BaseDbAction.Messages"/>.
    /// </remarks>
    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = GetDatabaseName(session);

            Catalog.CreateIndex(_model.ToIndexFile(), _model.TableName, databaseName);

            var tableDataRows = Context.GetTableContents(_model.TableName, databaseName);
            Dictionary<string, List<long>> indexValues = CreateIndexContents(tableDataRows);

            Indexes.CreateIndex(indexValues, _model.IndexName, _model.TableName, databaseName);

            Logger.Info($"New index file {_model.IndexName} successfully created!");
            Messages.Add($"New index file {_model.IndexName} successfully created!");
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add(ex.Message);
        }
    }

    /// <summary>
    /// Iterates over all existing rows in the table and builds a dictionary mapping
    /// composite index keys to their corresponding row IDs.
    /// </summary>
    /// <param name="tableData">All rows in the table, keyed by row ID with column name/value dictionaries as values.</param>
    /// <returns>
    /// A dictionary where each key is a composite index string (column values joined by <c>"##"</c>)
    /// and each value is a list of row IDs that share that key.
    /// </returns>
    private Dictionary<string, List<long>> CreateIndexContents(Dictionary<long, Dictionary<string, dynamic>> tableData)
    {
        Dictionary<string, List<long>> indexContentDict = [];

        foreach (KeyValuePair<long, Dictionary<string, dynamic>> row in tableData)
        {
            string key = ExtractIndexKeyFromRow(row.Value);

            if (indexContentDict.TryGetValue(key, out var rowIds))
            {
                rowIds.Add(row.Key);
            }
            else
            {
                indexContentDict.Add(key, [row.Key]);
            }
        }

        return indexContentDict;
    }

    /// <summary>
    /// Extracts the values of the indexed columns from a single row and concatenates them
    /// into a composite key string, using <c>"##"</c> as the delimiter between values.
    /// </summary>
    /// <param name="rowColumns">A dictionary of all column name/value pairs for a single row.</param>
    /// <returns>
    /// The composite index key string (e.g., <c>"Smith##John"</c> for a composite index on LastName and FirstName).
    /// Returns an empty string if none of the indexed columns are found in the row.
    /// </returns>
    private string ExtractIndexKeyFromRow(Dictionary<string, dynamic> rowColumns)
    {
        string key = string.Empty;

        foreach (KeyValuePair<string, dynamic> col in rowColumns)
        {
            if (_model.Attributes.Contains(col.Key))
            {
                key += col.Value + "##";
            }
        }

        if (key.Length > 0)
        {
            key = key.Remove(key.Length - 2, count: 2);
        }

        return key;
    }
}