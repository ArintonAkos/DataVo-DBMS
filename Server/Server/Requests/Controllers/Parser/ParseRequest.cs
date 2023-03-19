using Newtonsoft.Json;

namespace Server.Server.Requests.Controllers.Parser
{
    public class ParseRequest : Request
    {
        [JsonProperty("session")]
        public string Session { get; set; } = string.Empty;
    }
}
