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
/// Handles the <c>CREATE TABLE</c> DDL statement.
/// <para>
/// Creates both the logical table definition in the system catalog and the physical
/// storage allocation via the storage engine context. After table creation, automatically
/// generates a primary key (PK) B-Tree index and unique key (UK) B-Tree indexes for
/// any columns marked with those constraints.
/// </para>
/// </summary>
/// <param name="ast">The parsed <see cref="CreateTableStatement"/> AST node.</param>
/// <example>
/// <code>
/// var ast = new CreateTableStatement { TableName = "Users" };
/// var action = new CreateTable(ast);
/// action.PerformAction(sessionId);
/// // Output message: "Table Users successfully created!"
/// </code>
/// </example>
internal class CreateTable(CreateTableStatement ast) : BaseDbAction
{
    /// <summary>
    /// The parsed model containing the table name, column definitions, and constraint metadata.
    /// </summary>
    private readonly CreateTableModel _model = CreateTableModel.FromAst(ast);

    /// <summary>
    /// Executes the full table creation pipeline:
    /// <list type="number">
    ///   <item><description>Resolves the active database from the session cache.</description></item>
    ///   <item><description>Registers the table schema in the system catalog.</description></item>
    ///   <item><description>Allocates physical storage via the storage context.</description></item>
    ///   <item><description>Creates a PK index if the table declares primary key columns.</description></item>
    ///   <item><description>Creates UK indexes for each uniquely constrained column.</description></item>
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

            if (ast.IfNotExists && Catalog.TableExists(_model.TableName, databaseName))
            {
                Logger.Info($"Table {_model.TableName} already exists. Skipping creation.");
                Messages.Add($"Table {_model.TableName} already exists. Skipping creation.");
                return;
            }

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
    /// Creates a B-Tree index on the table's primary key columns.
    /// <para>
    /// The index is named <c>_PK_{TableName}</c> and is registered in both the system catalog
    /// and the <see cref="IndexManager"/>. If the table has no primary key, this method is a no-op.
    /// </para>
    /// </summary>
    /// <param name="databaseName">The name of the database containing the table.</param>
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
    /// Creates a separate B-Tree index for each column that has a UNIQUE constraint.
    /// <para>
    /// Each index is named <c>_UK_{ColumnName}</c> and is registered in both the system catalog
    /// and the <see cref="IndexManager"/>. These indexes are created with empty data since the
    /// table is newly created and contains no rows.
    /// </para>
    /// </summary>
    /// <param name="databaseName">The name of the database containing the table.</param>
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