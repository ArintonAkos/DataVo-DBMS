using Newtonsoft.Json;
using System;
using System.Net;

namespace Frontend.Client.Responses
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
