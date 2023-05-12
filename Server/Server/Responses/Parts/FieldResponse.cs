using Newtonsoft.Json;

namespace Server.Server.Responses.Parts;

public class FieldResponse
{
    [JsonProperty] public string FieldName { get; set; }
}