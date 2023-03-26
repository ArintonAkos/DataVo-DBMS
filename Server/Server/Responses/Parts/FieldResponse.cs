using Newtonsoft.Json;

namespace Server.Server.Responses.Parts
{
    internal class FieldResponse
    {
        [JsonProperty]
        public string FieldName { get; set; }
    }
}
