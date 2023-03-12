using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

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
