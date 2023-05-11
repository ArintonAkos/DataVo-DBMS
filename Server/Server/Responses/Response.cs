using Newtonsoft.Json;

namespace Server.Server.Responses;

internal class Response
{
    [JsonProperty("data")] public dynamic Data { get; set; }

    public string ToJson() => JsonConvert.SerializeObject(this, Formatting.Indented);
}