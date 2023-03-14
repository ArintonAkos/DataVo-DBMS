using Server.Logging;
using Server.Models;
using Server.Models.DDL;
using Server.Parser.Actions;
using Server.Server.MongoDB;
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
            try
            {
                Catalog.DropDatabase(_model.DatabaseName);

                DbContext.Instance.DropDatabase(_model.DatabaseName);

                Logger.Info(_model.DatabaseName);
                Messages.Add($"Database {_model.DatabaseName} successfully dropped!");
            }
            catch (Exception ex)
            {
                Logger.Error(ex.Message);
                Messages.Add(ex.Message);
            }
        }
    }
}
