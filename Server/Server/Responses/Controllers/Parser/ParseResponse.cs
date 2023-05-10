using Newtonsoft.Json;

namespace Server.Server.Responses.Controllers.Parser;

internal class ParseResponse : Response
{
    [JsonProperty("data")] public new List<ScriptResponse> Data { get; set; } = new();
}