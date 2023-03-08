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
        public HttpStatusCode Code { get; set; }
        [JsonProperty]
        public String Meta { get; set; }
        [JsonProperty]
        public String Data { get; set; }

        public String ToJson()
        {
            return JsonConvert.SerializeObject(this);
        }
    }
}
