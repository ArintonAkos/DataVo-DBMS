using Newtonsoft.Json;

namespace Frontend.Client.Responses
{
    public class Response
    {
        [JsonProperty("data")] public dynamic Data { get; set; }
    }
}