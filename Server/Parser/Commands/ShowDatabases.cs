using Server.Parser.Actions;
using System.Text.RegularExpressions;
using Server.Logging;
using Server.Models.Catalog;

namespace Server.Parser.Commands
{
    internal class ShowDatabases : BaseDbAction
    {
        public ShowDatabases(Match match) { }

        public override void PerformAction(Guid session)
        {
            try
            {
                Catalog.GetDatabases()
                .ForEach(databaseName => Fields.Add(new()
                {
                    FieldName = databaseName
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
