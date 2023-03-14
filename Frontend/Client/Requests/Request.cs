using Newtonsoft.Json;
using System;

namespace Frontend.Client.Requests
{
    internal class Request
    {
        [JsonProperty("data")]
        public String Data { get; set; }
    }
}
