using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Server.Responses.Controllers.Auth
{
    public class SessionResponse : Response
    {
        [JsonProperty("data")]
        public new Guid Data { get; set; }
    }
}
