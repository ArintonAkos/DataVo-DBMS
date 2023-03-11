using Server.Logging;
using Server.Models.DDL;
using Server.Parser.Actions;
using Server.Server.Responses;
using Server.Server.MongoDB;
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

        public override Response Perform()
        {
            if (_model.DatabaseName.ContainsAny("#", " ", ".", "/", "\\", "_"))
            {
                throw new Exception("Database Name Contains invalid characters!");
            }

            DbContext.Instance.GetDatabase(_model.DatabaseName);
            XML<Database>.InsertObjIntoXML(_model.ToDatabase(), "Databases", "databases", "Catalog.xml");
            Logger.Info($"Database with name: {_model.DatabaseName} successfully created!");

            return new Response()
            {
                Code = System.Net.HttpStatusCode.OK,
                Meta = "Database successfully created",
            };
        }
    }
}
