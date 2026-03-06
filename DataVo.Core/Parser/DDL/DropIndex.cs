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
/// Represents an index drop action.
/// Derived from <see cref="BaseDbAction"/>.
/// </summary>
/// <param name="ast">The AST representing the DROP INDEX statement.</param>
/// <example>
/// <code>
/// var ast = new DropIndexStatement { IndexName = "Idx_LastName", TableName = "Users" };
/// var action = new DropIndex(ast);
/// action.PerformAction(Guid.NewGuid());
/// </code>
/// </example>
internal class DropIndex(DropIndexStatement ast) : BaseDbAction
{
    private readonly DropIndexModel _model = DropIndexModel.FromAst(ast);

    /// <summary>
    /// Executes the deletion of an index from both the catalog and the physical B-Tree manager.
    /// </summary>
    /// <param name="session">The unique identifier of the user session executing the action.</param>
    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = CacheStorage.Get(session)
                ?? throw new Exception("No database in use!");

            Catalog.DropIndex(_model.IndexName, _model.TableName, databaseName);
            IndexManager.Instance.DropIndex(_model.IndexName, _model.TableName, databaseName);

            Logger.Info($"Index file {_model.IndexName} successfully dropped!");
            Messages.Add($"Index file {_model.IndexName} successfully dropped!");
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add(ex.Message);
        }
    }
}