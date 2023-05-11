using Newtonsoft.Json;

namespace Server.Server.Responses.Controllers.Parser;

public class ParseResponse : Response
{
    [JsonProperty("data")] public new List<ScriptResponse> Data { get; set; } = new();
}