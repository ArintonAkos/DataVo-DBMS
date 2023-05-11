using Newtonsoft.Json;

namespace Server.Server.Responses.Parts;

public class DataResponse
{
    [JsonProperty] public string Value { get; set; }

    [JsonProperty] public string FieldName { get; set; }
}