using Newtonsoft.Json;

namespace Server.Server.Responses
{
    internal class Response
    {
        [JsonProperty("data")]
        public dynamic Data { get; set; }

        public String ToJson()
        {
            return JsonConvert.SerializeObject(this, Formatting.Indented);
        }
    }
}
