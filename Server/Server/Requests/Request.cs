using Newtonsoft.Json;

namespace Server.Server.Requests
{
    internal class Request
    {
        [JsonProperty("data")]
        public String Data { get; set; }
    }
}
