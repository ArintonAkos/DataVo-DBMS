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
/// Handles the <c>DROP TABLE</c> DDL statement.
/// <para>
/// Removes a table from the database by first cascading the deletion to all
/// associated B-Tree indexes, then removing the table definition from the system
/// catalog and the physical storage from the storage context.
/// </para>
/// </summary>
/// <param name="ast">The parsed <see cref="DropTableStatement"/> AST node.</param>
/// <example>
/// <code>
/// var ast = new DropTableStatement { TableName = "Users" };
/// var action = new DropTable(ast);
/// action.PerformAction(sessionId);
/// // Output message: "Table Users successfully dropped!"
/// </code>
/// </example>
internal class DropTable(DropTableStatement ast) : BaseDbAction
{
    /// <summary>
    /// The parsed model containing the table name to drop.
    /// </summary>
    private readonly DropTableModel _model = DropTableModel.FromAst(ast);

    /// <summary>
    /// Executes the table drop pipeline:
    /// <list type="number">
    ///   <item><description>Resolves the active database from the session cache.</description></item>
    ///   <item><description>Drops all B-Tree indexes associated with the table.</description></item>
    ///   <item><description>Removes the table definition from the system catalog.</description></item>
    ///   <item><description>Removes the physical storage files via the storage context.</description></item>
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
            string databaseName = CacheStorage.Get(session)
                ?? throw new Exception("No database in use!");

            if (ast.IfExists && !Catalog.TableExists(_model.TableName, databaseName))
            {
                Logger.Info($"Table {_model.TableName} does not exist. Skipping drop.");
                Messages.Add($"Table {_model.TableName} does not exist. Skipping drop.");
                return;
            }

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
    /// Retrieves all indexes associated with the table from the system catalog
    /// and drops each one via <see cref="IndexManager"/>.
    /// This prevents orphaned index files from remaining after the table is deleted.
    /// </summary>
    /// <param name="databaseName">The name of the database containing the table.</param>
    private void DropAssociatedTableIndexes(string databaseName)
    {
        var indexes = Catalog.GetTableIndexes(_model.TableName, databaseName);

        foreach (var index in indexes)
        {
            IndexManager.Instance.DropIndex(index.IndexFileName, _model.TableName, databaseName);
        }
    }
}