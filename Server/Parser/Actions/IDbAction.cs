using Server.Server.Requests.Controllers.Parser;
using Server.Server.Responses;

namespace Server.Parser.Actions;

internal interface IDbAction
{
    public ActionResponse Perform(Guid session, ParseRequest? request = null);
}