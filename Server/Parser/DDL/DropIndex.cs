using System.Text.RegularExpressions;
using Server.Logging;
using Server.Models.Catalog;
using Server.Models.DDL;
using Server.Parser.Actions;
using Server.Server.Cache;
using Server.Server.MongoDB;

namespace Server.Parser.DDL;

internal class DropIndex : BaseDbAction
{
    private readonly DropIndexModel _model;

    public DropIndex(Match match) => _model = DropIndexModel.FromMatch(match);

    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = CacheStorage.Get(session)
                ?? throw new Exception("No database in use!");

            Logger.Info(_model.TableName);

            Catalog.DropIndex(_model.IndexName, _model.TableName, databaseName);

            DbContext.Instance.DropIndex(_model.IndexName, _model.TableName, databaseName);

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