using Newtonsoft.Json;
using System.Collections.Generic;

namespace Frontend.Client.Responses
{
    public class Response
    {
        [JsonProperty("data")]
        public List<ScriptResponse> Data { get; set; }
    }
}
