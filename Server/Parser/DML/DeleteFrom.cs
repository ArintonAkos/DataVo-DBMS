using System.Text.RegularExpressions;
using Server.Logging;
using Server.Models.Catalog;
using Server.Models.DML;
using Server.Parser.Actions;
using Server.Server.Cache;
using Server.Server.MongoDB;

namespace Server.Parser.DML;

internal class DeleteFrom : BaseDbAction
{
    private readonly DeleteFromModel _model;

    public DeleteFrom(Match match) => _model = DeleteFromModel.FromMatch(match);

    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = CacheStorage.Get(session)
                ?? throw new Exception("No database in use!");

            List<string> toBeDeleted = _model.WhereStatement.EvaluateWithoutJoin(_model.TableName, databaseName).ToList();

            DbContext.Instance.DeleteFormTable(toBeDeleted, _model.TableName, databaseName);

            Catalog.GetTableIndexes(_model.TableName, databaseName)
                .Select(e => e.IndexFileName)
                .ToList()
                .ForEach(indexFile =>
                {
                    DbContext.Instance.DeleteFromIndex(toBeDeleted, indexFile, _model.TableName, databaseName);
                });

            Logger.Info($"Rows affected: {toBeDeleted.Count}");
            Messages.Add($"Rows affected: {toBeDeleted.Count}");
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add(ex.Message);
        }
    }
}