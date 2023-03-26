using Newtonsoft.Json;
using System;

namespace Frontend.Client.Responses.Controllers.Auth
{
    internal class SessionResponse : Response
    {
        [JsonProperty("data")]
        public new Guid Data { get; set; }
    }
}
