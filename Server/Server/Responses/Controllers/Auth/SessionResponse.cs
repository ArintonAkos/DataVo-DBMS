using Newtonsoft.Json;

namespace Server.Server.Responses.Controllers.Auth;

public class SessionResponse : Response
{
    [JsonProperty("data")] public new Guid Data { get; set; }
}