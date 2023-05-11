using Newtonsoft.Json;

namespace Frontend.Client.Requests
{
    internal class Request
    {
        [JsonProperty("data")] public string Data { get; set; }
    }
}