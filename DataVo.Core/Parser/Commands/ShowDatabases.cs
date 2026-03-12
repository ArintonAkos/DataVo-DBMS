using DataVo.Core.Logging;
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
            Fields.Add("DatabaseName");

            Catalog.GetDatabases()
                .ForEach(databaseName => Data.Add(new Dictionary<string, dynamic>
                {
                    ["DatabaseName"] = databaseName,
                }));
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add(ex.Message);
        }
    }
}