using Server.Logging;
using Server.Models.DDL;
using Server.Parser.Actions;
using Server.Server.Responses;
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

        public override Response Perform()
        {
            // Drop MongoDb database
            Logger.Info(_model.TableName);


            return new Response();
        }
    }
}
