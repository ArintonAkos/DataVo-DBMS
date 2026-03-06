using System.Text.RegularExpressions;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Models.DDL;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;
using DataVo.Core.BTree;

namespace DataVo.Core.Parser.DDL;

/// <summary>
/// Represents a database drop action.
/// Derived from <see cref="BaseDbAction"/>.
/// </summary>
/// <param name="ast">The AST representing the DROP DATABASE statement.</param>
/// <example>
/// <code>
/// var ast = new DropDatabaseStatement { DatabaseName = "TestDb" };
/// var action = new DropDatabase(ast);
/// action.PerformAction(Guid.NewGuid());
/// </code>
/// </example>
internal class DropDatabase(DropDatabaseStatement ast) : BaseDbAction
{
    private readonly DropDatabaseModel _model = DropDatabaseModel.FromAst(ast);

    /// <summary>
    /// Executes the logical and physical deletion of the database.
    /// Removes the database from catalog, context, and drops its associated B-Tree indexes.
    /// </summary>
    /// <param name="session">The unique identifier of the user session executing the action.</param>
    public override void PerformAction(Guid session)
    {
        try
        {
            Catalog.DropDatabase(_model.DatabaseName);
            Context.DropDatabase(_model.DatabaseName);

            IndexManager.Instance.DropDatabaseIndexes(_model.DatabaseName);

            Logger.Info($"Database {_model.DatabaseName} successfully dropped!");
            Messages.Add($"Database {_model.DatabaseName} successfully dropped!");
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add(ex.Message);
        }
    }
}