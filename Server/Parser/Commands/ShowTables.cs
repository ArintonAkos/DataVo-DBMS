using Server.Parser.Actions;
using System.Text.RegularExpressions;
using Server.Logging;
using Server.Models.Catalog;

namespace Server.Parser.Commands
{
    internal class ShowTables : BaseDbAction
    {
        public ShowTables(Match match) { }

        public override void PerformAction(Guid session)
        {
            try
            {
                Catalog.GetTables("University")
                .ForEach(tableName => Fields.Add(new()
                {
                    FieldName = tableName
                }));
            }
            catch (Exception ex)
            {
                Logger.Error(ex.Message);
                Messages.Add(ex.Message);
            }
        }
    }
}
