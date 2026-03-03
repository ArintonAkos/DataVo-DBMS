using System.Text.RegularExpressions;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Models.DDL;
using DataVo.Core.Parser.Actions;
using DataVo.Core.BTree;
using DataVo.Core.Cache;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.DDL;

internal class CreateTable(CreateTableStatement ast) : BaseDbAction
{
    private readonly CreateTableModel _model = CreateTableModel.FromAst(ast);

    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = CacheStorage.Get(session)
                ?? throw new Exception("No database in use!");

            Catalog.CreateTable(_model.ToTable(), databaseName);

            Context.CreateTable(_model.TableName, databaseName);

            // Create PK index (catalog + B-Tree) so INSERT can enforce uniqueness
            // and FK validation on referencing tables works correctly
            List<string> primaryKeys = Catalog.GetTablePrimaryKeys(_model.TableName, databaseName);
            if (primaryKeys.Count != 0)
            {
                string pkIndexName = $"_PK_{_model.TableName}";
                var pkIndexFile = new IndexFile { IndexFileName = pkIndexName, AttributeNames = primaryKeys };
                Catalog.CreateIndex(pkIndexFile, _model.TableName, databaseName);
                IndexManager.Instance.CreateIndex([], pkIndexName, _model.TableName, databaseName);
            }

            // Create UK indexes (catalog + B-Tree) so INSERT can enforce uniqueness
            // and MakeInsertion populates them on every insert
            List<string> uniqueKeys = Catalog.GetTableUniqueKeys(_model.TableName, databaseName);
            uniqueKeys.ForEach(key =>
            {
                string ukIndexName = $"_UK_{key}";
                var ukIndexFile = new IndexFile { IndexFileName = ukIndexName, AttributeNames = [key] };
                Catalog.CreateIndex(ukIndexFile, _model.TableName, databaseName);
                IndexManager.Instance.CreateIndex([], ukIndexName, _model.TableName, databaseName);
            });

            Logger.Info($"New table {_model.TableName} successfully created!");
            Messages.Add($"Table {_model.TableName} successfully created!");
        }
        catch (Exception e)
        {
            Logger.Error(e.Message);
            Messages.Add($"Error: {e.Message}");
        }
    }
}