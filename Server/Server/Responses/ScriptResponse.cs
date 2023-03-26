using Newtonsoft.Json;

namespace Server.Server.Responses
{
    internal class ScriptResponse
    {
        [JsonProperty]
        public List<ActionResponse> Actions { get; set; } = new();

        [JsonProperty]
        public Boolean IsSuccess { get; set; } = true;
    }
}
