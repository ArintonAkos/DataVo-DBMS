using Server.Logging;
using Server.Models;
using Server.Models.DDL;
using Server.Parser.Actions;
using System.Text.RegularExpressions;
using Server.Utils;
using Server.Models;

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
            if (_model.DatabaseName.ContainsAny("#", " ", ".", "/", "\\", "_"))
            {
                throw new Exception("Database Name Contains invalid characters!");
            }

            XML.InsertObjIntoXML(_model.ToDatabase(), "Databases", "databases", "Catalog.xml");

            Messages.Add($"Database {_model.DatabaseName} successfully created!");
        }
    }
}
