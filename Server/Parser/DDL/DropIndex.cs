using Server.Logging;
using Server.Models.Catalog;
using Server.Models.DDL;
using Server.Parser.Actions;
using System.Text.RegularExpressions;

namespace Server.Parser.DDL
{
    internal class DropIndex : BaseDbAction
    {
        private readonly DropIndexModel _model;

        public DropIndex(Match match)
        {
            _model = DropIndexModel.FromMatch(match);
        }

        public override void PerformAction(string session)
        {
            try
            {
                Logger.Info(_model.TableName);

                Catalog.DropIndex(_model.IndexName, _model.TableName, "University");

                Logger.Info($"Index file {_model.IndexName} successfully dropped!");
                Messages.Add($"Index file {_model.IndexName} successfully dropped!");
            }
            catch (Exception ex)
            {
                Logger.Error(ex.Message);
                Messages.Add(ex.Message);
            }
        }
    }
}
