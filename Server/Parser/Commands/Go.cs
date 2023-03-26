using Server.Parser.Actions;
using Server.Server.Responses;
using System.Text.RegularExpressions;

namespace Server.Parser.Commands
{
    internal class Go : IDbAction
    {
        public Go(Match match) { }
        public ActionResponse Perform(Guid session)
        {
            return ActionResponse.Default();
        }
    }
}
