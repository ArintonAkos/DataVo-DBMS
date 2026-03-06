using System.Text.RegularExpressions;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Models.DDL;
using DataVo.Core.Parser.Actions;
using DataVo.Core.BTree;
using DataVo.Core.Cache;
using DataVo.Core.StorageEngine;
using DataVo.Core.Parser.AST;
using DataVo.Core.Models.Statement.Utils;

namespace DataVo.Core.Parser.DDL;

/// <summary>
/// Represents an index creation action.
/// Derived from <see cref="BaseDbAction"/>.
/// </summary>
/// <param name="ast">The AST representing the CREATE INDEX statement.</param>
/// <example>
/// <code>
/// var ast = new CreateIndexStatement { IndexName = "Idx_LastName", TableName = "Users" };
/// var action = new CreateIndex(ast);
/// action.PerformAction(Guid.NewGuid());
/// </code>
/// </example>
internal class CreateIndex(CreateIndexStatement ast) : BaseDbAction
{
    private readonly CreateIndexModel _model = CreateIndexModel.FromAst(ast);

    /// <summary>
    /// Executes the creation of a new index within the database catalog 
    /// and populates it with existing table data.
    /// </summary>
    /// <param name="session">The unique identifier of the user session executing the action.</param>
    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = CacheStorage.Get(session)
                ?? throw new Exception("No database in use!");

            Catalog.CreateIndex(_model.ToIndexFile(), _model.TableName, databaseName);

            var tableDataRows = StorageContext.Instance.GetTableContents(_model.TableName, databaseName);
            Dictionary<string, List<long>> indexValues = CreateIndexContents(tableDataRows);

            IndexManager.Instance.CreateIndex(indexValues, _model.IndexName, _model.TableName, databaseName);

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
    /// Scans the currently existing rows in the table to dynamically build the index dictionary.
    /// Extracts column data relevant to the index and concatenates it to serve as the dictionary key.
    /// </summary>
    /// <param name="tableData">The physical table row contents mapped by their primary 64-bit bounds.</param>
    /// <returns>A dictionary mapping the generated index keys to row IDs.</returns>
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
    /// Extracts the composite index string key from the target columns within a single row.
    /// Columns are separated by '##'.
    /// </summary>
    /// <param name="rowColumns">The dictionary containing all attribute names and values for a distinct row.</param>
    /// <returns>A formatted index key string reflecting the target attributes.</returns>
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