using Newtonsoft.Json;

namespace Server.Server.Responses.Parts
{
    internal class ActionDataResponse
    {
        [JsonProperty]
        public List<List<DataResponse>> Data { get; set; } = new();
    }
}
