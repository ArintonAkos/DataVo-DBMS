using System.Text.RegularExpressions;
using Server.Logging;
using Server.Models.Catalog;
using Server.Models.DDL;
using Server.Parser.Actions;
using Server.Server.MongoDB;

namespace Server.Parser.DDL;

internal class DropTable : BaseDbAction
{
    private readonly DropTableModel _model;

    public DropTable(Match match)
    {
        _model = DropTableModel.FromMatch(match);
    }

    public override void PerformAction(Guid session)
    {
        try
        {
            Catalog.GetTableIndexes(_model.TableName, "University")
                .Select(e => e.IndexFileName)
                .ToList()
                .ForEach(indexFile => { DbContext.Instance.DropIndex(indexFile, _model.TableName, "University"); });

            Catalog.DropTable(_model.TableName, "University");

            DbContext.Instance.DropTable(_model.TableName, "University");

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