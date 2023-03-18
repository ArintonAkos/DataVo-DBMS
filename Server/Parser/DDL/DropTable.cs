using Server.Logging;
using Server.Models.Catalog;
using Server.Models.DDL;
using Server.Parser.Actions;
using Server.Server.MongoDB;
using System.Text.RegularExpressions;

namespace Server.Parser.DDL
{
    internal class DropTable : BaseDbAction
    {
        private readonly DropTableModel _model;

        public DropTable(Match match)
        {
            _model = DropTableModel.FromMatch(match);
        }

        public override void PerformAction()
        {
            try
            {
                Catalog.DropTable(_model.TableName, "University");

                DbContext.Instance.DropTable(_model.TableName, "University");

                Logger.Info(_model.TableName);
                Messages.Add($"Table {_model.TableName} successfully dropped!");
            }
            catch (Exception ex) 
            {
                Logger.Error(ex.Message);
                Messages.Add(ex.Message);
            }
        }
    }
}
