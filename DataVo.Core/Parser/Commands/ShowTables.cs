using DataVo.Core.Logging;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.Commands;

internal class ShowTables : BaseDbAction
{
    public ShowTables(ShowTablesStatement _) { }
    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = GetDatabaseName(session);
            Fields.Add("TableName");

            Catalog.GetTables(databaseName)
                .ForEach(tableName => Data.Add(new Dictionary<string, dynamic>
                {
                    ["TableName"] = tableName,
                }));
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add(ex.Message);
        }
    }
}