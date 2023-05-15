using System.Text.RegularExpressions;
using Server.Parser.Actions;
using Server.Server.Responses;

namespace Server.Parser.Commands;

internal class Go : IDbAction
{
    public Go(Match match)
    { }

    public ActionResponse Perform(Guid session) => ActionResponse.Default();
}