using Newtonsoft.Json;
using Server.Models;
using Server.Server.Responses.Parts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

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
