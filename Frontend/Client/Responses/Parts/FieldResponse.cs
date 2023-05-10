using Newtonsoft.Json;

namespace Frontend.Client.Responses.Parts
{
    internal class FieldResponse
    {
        [JsonProperty] public string FieldName { get; set; }
    }
}