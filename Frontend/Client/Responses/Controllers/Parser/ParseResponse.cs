using Newtonsoft.Json;
using System.Collections.Generic;

namespace Frontend.Client.Responses.Controllers.Parser
{
    internal class ParseResponse : Response
    {
        [JsonProperty("data")]
        public new List<ScriptResponse> Data { get; set; } = new List<ScriptResponse>();
    }
}
