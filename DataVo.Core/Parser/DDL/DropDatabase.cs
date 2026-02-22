using System.Text.RegularExpressions;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Models.DDL;
using DataVo.Core.Parser.Actions;
using DataVo.Core.MongoDB;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.DDL;

internal class DropDatabase : BaseDbAction
{
    private readonly DropDatabaseModel _model;

    public DropDatabase(Match match) => _model = DropDatabaseModel.FromMatch(match);
    public DropDatabase(DropDatabaseStatement ast) => _model = DropDatabaseModel.FromAst(ast);

    public override void PerformAction(Guid session)
    {
        try
        {
            Catalog.DropDatabase(_model.DatabaseName);

            DbContext.Instance.DropDatabase(_model.DatabaseName);

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