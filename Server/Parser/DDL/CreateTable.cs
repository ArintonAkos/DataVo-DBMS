using System.Text.RegularExpressions;
using MongoDB.Bson;
using Server.Logging;
using Server.Models.Catalog;
using Server.Models.DDL;
using Server.Parser.Actions;
using Server.Server.MongoDB;

namespace Server.Parser.DDL;

internal class CreateTable : BaseDbAction
{
    private readonly CreateTableModel _model;

    public CreateTable(Match match)
    {
        _model = CreateTableModel.FromMatch(match);
    }

    public override void PerformAction(Guid session)
    {
        try
        {
            Catalog.CreateTable(_model.ToTable(), "University");

            DbContext.Instance.CreateTable(_model.TableName, "University");

            var uniqueKeys = Catalog.GetTableUniqueKeys(_model.TableName, "University");
            uniqueKeys.ForEach(key =>
            {
                DbContext.Instance.CreateIndex(new List<BsonDocument>(), $"_UK_{key}", _model.TableName,
                    "University");
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