using Server.Logging;
using Server.Models.DDL;
using Server.Parser.Actions;
using System.Text.RegularExpressions;

namespace Server.Parser.DDL
{
    internal class DropDatabase : BaseDbAction
    {
        private readonly DropDatabaseModel _model;

        public DropDatabase(Match match)
        {
            _model = DropDatabaseModel.FromMatch(match);
        }

        public override void PerformAction()
        {
            // Drop MongoDb database
            Logger.Info(_model.DatabaseName);

            Messages.Add($"Database {_model.DatabaseName} successfully dropped!");
        }
    }
}
