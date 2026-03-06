using System.Text.RegularExpressions;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Models.DDL;
using DataVo.Core.Parser.Actions;
using DataVo.Core.BTree;
using DataVo.Core.Cache;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.DDL;

/// <summary>
/// Represents a table drop action.
/// Derived from <see cref="BaseDbAction"/>.
/// </summary>
/// <param name="ast">The AST representing the DROP TABLE statement.</param>
/// <example>
/// <code>
/// var ast = new DropTableStatement { TableName = "Users" };
/// var action = new DropTable(ast);
/// action.PerformAction(Guid.NewGuid());
/// </code>
/// </example>
internal class DropTable(DropTableStatement ast) : BaseDbAction
{
    private readonly DropTableModel _model = DropTableModel.FromAst(ast);

    /// <summary>
    /// Executes the logical and physical deletion of the table.
    /// Also drops all indexes associated with the table before dropping the table itself.
    /// </summary>
    /// <param name="session">The unique identifier of the user session executing the action.</param>
    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = CacheStorage.Get(session)
                ?? throw new Exception("No database in use!");

            DropAssociatedTableIndexes(databaseName);

            Catalog.DropTable(_model.TableName, databaseName);
            Context.DropTable(_model.TableName, databaseName);

            Logger.Info($"Table {_model.TableName} successfully dropped!");
            Messages.Add($"Table {_model.TableName} successfully dropped!");
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add($"Error: {ex.Message}");
        }
    }

    /// <summary>
    /// Drops all B-Tree indexes associated with the given table.
    /// Retrieves the list of indexes from the catalog and interacts with the index manager.
    /// </summary>
    /// <param name="databaseName">The database name where the table and indexes belong.</param>
    private void DropAssociatedTableIndexes(string databaseName)
    {
        var indexes = Catalog.GetTableIndexes(_model.TableName, databaseName);
        
        foreach (var index in indexes)
        {
            IndexManager.Instance.DropIndex(index.IndexFileName, _model.TableName, databaseName);
        }
    }
}