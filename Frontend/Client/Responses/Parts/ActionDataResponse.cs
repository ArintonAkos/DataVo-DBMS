using Newtonsoft.Json;
using System.Collections.Generic;

namespace Frontend.Client.Responses.Parts
{
    internal class ActionDataResponse
    {
        [JsonProperty]
        public List<List<DataResponse>> Data { get; set; } = new List<List<DataResponse>>();
    }
}
