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

            List<string> uniqueKeys = Catalog.GetTableUniqueKeys(_model.TableName, databaseName);
            uniqueKeys.ForEach(key =>
            {
                IndexManager.Instance.CreateIndex([], $"_UK_{key}", _model.TableName, databaseName);
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