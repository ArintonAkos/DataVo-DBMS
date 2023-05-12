using System.Text.RegularExpressions;
using Server.Models.DML;
using Server.Parser.Actions;

namespace Server.Parser.DML;

internal class DeleteFrom : BaseDbAction
{
    private readonly DeleteFromModel _model;

    public DeleteFrom(Match match) => _model = DeleteFromModel.FromMatch(match);

    public override void PerformAction(Guid session)
    {
        try
        {
            List<string> toBeDeleted = _model.WhereStatement.Evaluate(_model.TableName, "University").ToList();

            DbContext.Instance.DeleteFormTable(toBeDeleted, _model.TableName, "University");

            Catalog.GetTableIndexes(_model.TableName, "University")
            .Select(e => e.IndexFileName)
            .ToList()
            .ForEach(indexFile =>
            {
                DbContext.Instance.DeleteFromIndex(toBeDeleted, indexFile, _model.TableName, "University");
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