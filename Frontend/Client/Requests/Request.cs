using Newtonsoft.Json;
using System;

namespace Frontend.Client.Requests
{
    internal class Request
    {
        [JsonProperty("command_type")]
        public String CommandType { get; set; }
        [JsonProperty("data")]
        public String Data { get; set; }
    }
}
