using System.Collections.Generic;
using Newtonsoft.Json;

namespace Frontend.Client.Responses.Parts
{
    internal class ActionFieldResponse
    {
        [JsonProperty] public List<FieldResponse> Fields { get; set; } = new List<FieldResponse>();
    }
}