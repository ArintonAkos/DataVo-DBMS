using Newtonsoft.Json;

namespace Server.Server.Responses.Parts;

internal class ActionFieldResponse
{
    [JsonProperty] public List<FieldResponse> Fields { get; set; } = new();
}