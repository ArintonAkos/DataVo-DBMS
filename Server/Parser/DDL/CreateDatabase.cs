using System.Text.RegularExpressions;
using Server.Logging;
using Server.Models.Catalog;
using Server.Models.DDL;
using Server.Parser.Actions;
using Server.Server.Requests.Controllers.Parser;

namespace Server.Parser.DDL;

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