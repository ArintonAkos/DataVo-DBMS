using System.Text.RegularExpressions;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Models.DDL;
using DataVo.Core.Parser.Actions;
using DataVo.Core.BTree;
using DataVo.Core.Cache;
using DataVo.Core.MongoDB;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.DDL;

internal class DropTable : BaseDbAction
{
    private readonly DropTableModel _model;

    public DropTable(Match match) => _model = DropTableModel.FromMatch(match);
    public DropTable(DropTableStatement ast) => _model = DropTableModel.FromAst(ast);

    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = CacheStorage.Get(session)
                ?? throw new Exception("No database in use!");

            Catalog.GetTableIndexes(_model.TableName, databaseName)
                .Select(e => e.IndexFileName)
                .ToList()
                .ForEach(indexFile => { IndexManager.Instance.DropIndex(indexFile, _model.TableName, databaseName); });

            Catalog.DropTable(_model.TableName, databaseName);

            DbContext.Instance.DropTable(_model.TableName, databaseName);

            Logger.Info($"Table {_model.TableName} successfully dropped!");
            Messages.Add($"Table {_model.TableName} successfully dropped!");
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add(ex.Message);
        }
    }
}