using Server.Logging;
using Server.Models.DDL;
using Server.Parser.Actions;
using Server.Server.Responses;
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

        public override Response Perform()
        {
            // Create MongoDb database
            Logger.Info(_model.DatabaseName);

            return new Response();
        }
    }
}
