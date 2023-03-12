using Server.Logging;
using Server.Models.DDL;
using Server.Parser.Actions;
using System.Text.RegularExpressions;

namespace Server.Parser.DDL
{
    internal class CreateDatabase : BaseDbAction
    {
        private readonly CreateDatabaseModel _model;

        public CreateDatabase(Match match)
        {
            _model = CreateDatabaseModel.FromMatch(match);
        }

        public override void PerformAction()
        {
            // Create MongoDb database
            Logger.Info(_model.DatabaseName);

            Messages.Add($"Database {_model.DatabaseName} successfully created!");
        }
    }
}
