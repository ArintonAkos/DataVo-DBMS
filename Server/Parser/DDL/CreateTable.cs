using System.Text.RegularExpressions;
using MongoDB.Bson;
using Server.Logging;
using Server.Models.Catalog;
using Server.Models.DDL;
using Server.Parser.Actions;
using Server.Server.Cache;
using Server.Server.MongoDB;
using Server.Server.Requests.Controllers.Parser;

namespace Server.Parser.DDL;

internal class CreateTable : BaseDbAction
{
    private readonly CreateTableModel _model;

    public CreateTable(Match match) => _model = CreateTableModel.FromMatch(match);

    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = CacheStorage.Get(session)
                ?? throw new Exception("No database in use!");

            Catalog.CreateTable(_model.ToTable(), databaseName);

            DbContext.Instance.CreateTable(_model.TableName, databaseName);

            List<string> uniqueKeys = Catalog.GetTableUniqueKeys(_model.TableName, databaseName);
            uniqueKeys.ForEach(key =>
            {
                DbContext.Instance.CreateIndex(new List<BsonDocument>(), $"_UK_{key}", _model.TableName, databaseName);
            });

            Logger.Info($"New table {_model.TableName} successfully created!");
            Messages.Add($"Table {_model.TableName} successfully created!");
        }
        catch (Exception e)
        {
            Logger.Error(e.Message);
            Messages.Add(e.Message);
        }
    }
}