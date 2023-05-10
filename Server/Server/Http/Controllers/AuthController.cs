using Server.Server.Http.Attributes;
using Server.Server.Responses.Controllers.Auth;

namespace Server.Server.Http.Controllers;

[Route("auth")]
internal class AuthController
{
    [Method("GET")]
    [Route("session")]
    public static SessionResponse Authenticate()
    {
        return new SessionResponse
        {
            Data = Guid.NewGuid()
        };
    }
}