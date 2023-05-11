using Newtonsoft.Json;

namespace Frontend.Client.Responses
{
    internal class Response
    {
        [JsonProperty("data")] public dynamic Data { get; set; }
    }
}