using Server.Models.DDL;
using Server.Logging;
using Server.Parser.Actions;
using System.Text.RegularExpressions;
using Server.Models.Catalog;

namespace Server.Parser.DDL
{
    internal class CreateIndex : BaseDbAction
    {
        private readonly CreateIndexModel _model;

        public CreateIndex(Match match)
        {
            _model = CreateIndexModel.FromMatch(match);
        }

        public override void PerformAction(string session)
        {
            try
            {
                Catalog.CreateIndex(_model.ToIndexFile(), _model.TableName, "University");

                Logger.Info($"New index file {_model.IndexName} successfully created!");
                Messages.Add($"New index file {_model.IndexName} successfully created!");
            }
            catch (Exception ex) 
            {
                Logger.Error(ex.Message);
                Messages.Add(ex.Message);
            }
        }
    }
}
