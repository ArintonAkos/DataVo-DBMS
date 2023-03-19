using Server.Logging;
using Server.Models;
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

        public override void PerformAction(string session)
        {
            try
            {
                Catalog.CreateDatabase(_model.ToDatabase());

                Logger.Info($"New database {_model.DatabaseName} successfully created!");
                Messages.Add($"Database {_model.DatabaseName} successfully created!");
            }
            catch (Exception ex) 
            {
                Logger.Error(ex.Message);
                Messages.Add(ex.Message);
            }
        }
    }
}
