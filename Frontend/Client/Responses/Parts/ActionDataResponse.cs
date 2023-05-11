using System.Collections.Generic;
using Newtonsoft.Json;

namespace Frontend.Client.Responses.Parts
{
    public class ActionDataResponse
    {
        [JsonProperty] public List<List<DataResponse>> Data { get; set; } = new List<List<DataResponse>>();
    }
}