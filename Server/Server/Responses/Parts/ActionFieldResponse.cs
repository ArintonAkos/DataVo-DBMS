using Newtonsoft.Json;

namespace Server.Server.Responses.Parts
{
    public class ActionFieldResponse
    {
        [JsonProperty]
        public List<FieldResponse> Fields { get; set; } = new();
    }
}
