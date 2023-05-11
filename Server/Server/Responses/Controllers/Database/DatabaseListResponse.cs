using Newtonsoft.Json;
using Server.Server.Responses.Parts;

namespace Server.Server.Responses.Controllers.Database
{
    public class DatabaseListResponse : Response
    {
        [JsonProperty("data")] public new List<DatabaseModel> Data { get; set; } = new();
    }
}
