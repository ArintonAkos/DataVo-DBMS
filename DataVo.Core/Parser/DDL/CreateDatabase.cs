using System.Text.RegularExpressions;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Models.DDL;
using DataVo.Core.Parser.Actions;

namespace DataVo.Core.Parser.DDL;

internal class CreateDatabase : BaseDbAction
{
    private readonly CreateDatabaseModel _model;

    public CreateDatabase(Match match) => _model = CreateDatabaseModel.FromMatch(match);

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