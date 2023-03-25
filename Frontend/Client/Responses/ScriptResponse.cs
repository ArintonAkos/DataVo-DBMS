using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace Frontend.Client.Responses
{
    public class ScriptResponse
    {
        [JsonProperty]
        public List<ActionResponse> Actions { get; set; } = new List<ActionResponse>();

        [JsonProperty]
        public Boolean IsSuccess { get; set; } = true;
    }
}
