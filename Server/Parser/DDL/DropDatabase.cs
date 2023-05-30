using System.Text.RegularExpressions;
using Server.Logging;
using Server.Models.Catalog;
using Server.Models.DDL;
using Server.Parser.Actions;
using Server.Server.MongoDB;
using Server.Server.Requests.Controllers.Parser;

namespace Server.Parser.DDL;

internal class DropDatabase : BaseDbAction
{
    private readonly DropDatabaseModel _model;

    public DropDatabase(Match match, ParseRequest request) => _model = DropDatabaseModel.FromMatch(match);

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