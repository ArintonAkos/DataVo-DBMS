using Server.Logging;
using Server.Models;
using Server.Models.DDL;
using Server.Parser.Actions;
using Server.Utils;
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

            XML<Database>.InsertObjIntoXML(_model.ToDatabase(), "Databases", "databases", "Catalog.xml");

            Messages.Add($"Database {_model.DatabaseName} successfully created!");
        }
    }
}
