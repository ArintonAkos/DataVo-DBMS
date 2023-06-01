using System.Text.RegularExpressions;
using Server.Parser.Actions;
using Server.Server.Requests.Controllers.Parser;
using Server.Server.Responses;

namespace Server.Parser.Commands;

internal class Go : IDbAction
{
    public Go(Match _)
    {
    }

    public ActionResponse Perform(Guid session) => ActionResponse.Default();
}