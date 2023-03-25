using Newtonsoft.Json;

namespace Frontend.Client.Responses
{
    public class Response
    {
        [JsonProperty("data")]
        public ScriptResponse Data { get; set; }
    }
}
