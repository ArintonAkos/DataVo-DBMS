using Newtonsoft.Json;

namespace Frontend.Client.Responses.Parts
{
    public class FieldResponse
    {
        [JsonProperty] public string FieldName { get; set; }
    }
}