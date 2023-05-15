using System.Text.RegularExpressions;
using Server.Logging;
using Server.Models.Catalog;
using Server.Parser.Actions;
using Server.Server.Cache;
using Server.Server.Responses.Parts;

namespace Server.Parser.Commands;

internal class ShowTables : BaseDbAction
{
    public ShowTables(Match match)
    {
    }

    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = CacheStorage.Get(session)
                ?? throw new Exception("No database in use!");

            Catalog.GetTables(databaseName)
            .ForEach(tableName => Fields.Add(new FieldResponse
            {
                FieldName = tableName,
            }));
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add(ex.Message);
        }
    }
}