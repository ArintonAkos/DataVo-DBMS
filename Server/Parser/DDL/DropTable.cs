using System.Text.RegularExpressions;
using Server.Logging;
using Server.Models.Catalog;
using Server.Models.DDL;
using Server.Parser.Actions;
using Server.Server.Cache;
using Server.Server.MongoDB;
using Server.Server.Requests.Controllers.Parser;

namespace Server.Parser.DDL;

internal class DropTable : BaseDbAction
{
    private readonly DropTableModel _model;

    public DropTable(Match match, ParseRequest request) => _model = DropTableModel.FromMatch(match);

    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = CacheStorage.Get(session)
                ?? throw new Exception("No database in use!");

            Catalog.GetTableIndexes(_model.TableName, databaseName)
                .Select(e => e.IndexFileName)
                .ToList()
                .ForEach(indexFile => { DbContext.Instance.DropIndex(indexFile, _model.TableName, databaseName); });

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