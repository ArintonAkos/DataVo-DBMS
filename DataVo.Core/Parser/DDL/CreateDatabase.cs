using System.Text.RegularExpressions;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Models.DDL;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.DDL;

/// <summary>
/// Represents a database creation action.
/// Derived from <see cref="BaseDbAction"/>.
/// </summary>
/// <param name="ast">The AST representing the CREATE DATABASE statement.</param>
/// <example>
/// <code>
/// var ast = new CreateDatabaseStatement { DatabaseName = "TestDb" };
/// var action = new CreateDatabase(ast);
/// action.PerformAction(Guid.NewGuid());
/// </code>
/// </example>
internal class CreateDatabase(CreateDatabaseStatement ast) : BaseDbAction
{
    private readonly CreateDatabaseModel _model = CreateDatabaseModel.FromAst(ast);

    /// <summary>
    /// Executes the logical creation of the database in the catalog.
    /// </summary>
    /// <param name="session">The unique identifier of the user session executing the action.</param>
    public override void PerformAction(Guid session)
    {
        try
        {
            Catalog.CreateDatabase(_model.ToDatabase());

            Logger.Info($"New database {_model.DatabaseName} successfully created!");
            Messages.Add($"Database {_model.DatabaseName} successfully created!");
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add(ex.Message);
        }
    }
}