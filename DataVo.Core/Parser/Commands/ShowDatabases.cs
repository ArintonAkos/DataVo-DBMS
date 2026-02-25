using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.Commands;

internal class ShowDatabases : BaseDbAction
{
    public ShowDatabases(ShowDatabasesStatement _) { }
    public override void PerformAction(Guid session)
    {
        try
        {
            Catalog.GetDatabases()
                .ForEach(Fields.Add);
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add(ex.Message);
        }
    }
}