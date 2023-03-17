using Newtonsoft.Json;

namespace Server.Server.Responses
{
    internal class Response
    {
        [JsonProperty]
        public List<ScriptResponse> Data { get; set; } = new();

        public String ToJson()
        {
            return JsonConvert.SerializeObject(this, Formatting.Indented);
        }
    }
}
