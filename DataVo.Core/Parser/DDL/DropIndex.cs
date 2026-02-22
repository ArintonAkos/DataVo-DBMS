using System.Text.RegularExpressions;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Models.DDL;
using DataVo.Core.Parser.Actions;
using DataVo.Core.BTree;
using DataVo.Core.Cache;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.DDL;

internal class DropIndex : BaseDbAction
{
    private readonly DropIndexModel _model;

    public DropIndex(Match match, string query) => _model = DropIndexModel.FromMatch(match);
    public DropIndex(DropIndexStatement ast) => _model = DropIndexModel.FromAst(ast);

    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = CacheStorage.Get(session)
                ?? throw new Exception("No database in use!");

            Logger.Info(_model.TableName);

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