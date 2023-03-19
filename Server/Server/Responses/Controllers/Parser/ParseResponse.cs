using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Server.Responses.Controllers.Parser
{
    class ParseResponse : Response
    {
        [JsonProperty("data")]
        public new List<ScriptResponse> Data { get; set; } = new();
    }
}
