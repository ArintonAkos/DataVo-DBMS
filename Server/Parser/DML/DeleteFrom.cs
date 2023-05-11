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
        // try
        // {
        //     HashSet<string> indexedColumns = GetIndexedColumns(_model.TableName, "University");
        //
        //     List<string> toBeDeleted = _model.WhereStatement.Evaluate(indexedColumns).Select(row => row.);
        //
        //     DbContext.Instance.DeleteFromTable(toBeDeleted, _model.TableName, "University");
        //
        //     Catalog.GetTableIndexes(_model.TableName, "University")
        //         .Select(e => e.IndexFileName)
        //         .ToList()
        //         .ForEach(indexFile =>
        //         {
        //             DbContext.Instance.DeleteFromIndex(toBeDeleted, indexFile, _model.TableName, "University");
        //         });
        //
        //     Logger.Info($"Rows affected: {toBeDeleted.Count}");
        //     Messages.Add($"Rows affected: {toBeDeleted.Count}");
        // }
        // catch (Exception ex)
        // {
        //     Logger.Error(ex.Message);
        //     Messages.Add(ex.Message);
        // }
    }
}