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
/// Represents a table creation action statement handler.
/// Derived from <see cref="BaseDbAction"/>.
/// </summary>
/// <param name="ast">The AST representing the CREATE TABLE statement.</param>
/// <example>
/// <code>
/// var ast = new CreateTableStatement { TableName = "Users" };
/// var action = new CreateTable(ast);
/// action.PerformAction(Guid.NewGuid());
/// </code>
/// </example>
internal class CreateTable(CreateTableStatement ast) : BaseDbAction
{
    private readonly CreateTableModel _model = CreateTableModel.FromAst(ast);

    /// <summary>
    /// Executes the logical and physical creation of the table within the active database.
    /// Automatically manages the creation of primary and unique key indexes alongside.
    /// </summary>
    /// <param name="session">The unique identifier of the user session executing the action.</param>
    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = CacheStorage.Get(session)
                ?? throw new Exception("No database in use!");

            Catalog.CreateTable(_model.ToTable(), databaseName);
            Context.CreateTable(_model.TableName, databaseName);

            CreatePrimaryKeyIndex(databaseName);
            CreateUniqueKeyIndexes(databaseName);

            Logger.Info($"New table {_model.TableName} successfully created!");
            Messages.Add($"Table {_model.TableName} successfully created!");
        }
        catch (Exception e)
        {
            Logger.Error(e.Message);
            Messages.Add($"Error: {e.Message}");
        }
    }

    /// <summary>
    /// Creates a Primary Key (PK) index, using both the catalog and the B-Tree index manager.
    /// This allows <c>INSERT</c> operations to enforce uniqueness and referential integrity.
    /// </summary>
    /// <param name="databaseName">The active database name where the table belongs.</param>
    private void CreatePrimaryKeyIndex(string databaseName)
    {
        List<string> primaryKeys = Catalog.GetTablePrimaryKeys(_model.TableName, databaseName);
        if (primaryKeys.Count == 0) return;

        string pkIndexName = $"_PK_{_model.TableName}";
        var pkIndexFile = new IndexFile { IndexFileName = pkIndexName, AttributeNames = primaryKeys };
        
        Catalog.CreateIndex(pkIndexFile, _model.TableName, databaseName);
        IndexManager.Instance.CreateIndex([], pkIndexName, _model.TableName, databaseName);
    }

    /// <summary>
    /// Creates Unique Key (UK) indexes for all uniquely constrained columns 
    /// within the table via the catalog and the B-Tree index manager.
    /// </summary>
    /// <param name="databaseName">The active database name where the table belongs.</param>
    private void CreateUniqueKeyIndexes(string databaseName)
    {
        List<string> uniqueKeys = Catalog.GetTableUniqueKeys(_model.TableName, databaseName);
        uniqueKeys.ForEach(key =>
        {
            string ukIndexName = $"_UK_{key}";
            var ukIndexFile = new IndexFile { IndexFileName = ukIndexName, AttributeNames = [key] };
            
            Catalog.CreateIndex(ukIndexFile, _model.TableName, databaseName);
            IndexManager.Instance.CreateIndex([], ukIndexName, _model.TableName, databaseName);
        });
    }
}