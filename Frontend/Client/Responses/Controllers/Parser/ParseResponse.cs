using System.Collections.Generic;
using Newtonsoft.Json;

namespace Frontend.Client.Responses.Controllers.Parser
{
    internal class ParseResponse : Response
    {
        [JsonProperty("data")] public new List<ScriptResponse> Data { get; set; } = new List<ScriptResponse>();
    }
}