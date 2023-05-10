using Newtonsoft.Json;

namespace Server.Server.Responses;

internal class ScriptResponse
{
    [JsonProperty] public List<ActionResponse> Actions { get; set; } = new();

    [JsonProperty] public bool IsSuccess { get; set; } = true;
}