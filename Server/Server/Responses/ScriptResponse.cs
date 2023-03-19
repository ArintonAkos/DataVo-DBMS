using Newtonsoft.Json;

namespace Server.Server.Responses
{
    class ScriptResponse
    {
        [JsonProperty]
        public List<ActionResponse> Actions { get; set; } = new();

        [JsonProperty]
        public Boolean IsSuccess { get; set; } = true;
    }
}
