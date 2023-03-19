using MongoDB.Bson.IO;
using Server.Server.Http.Attributes;
using Server.Server.Responses;
using Server.Server.Responses.Controllers.Auth;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Server.Server.Http.Controllers
{
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
}
