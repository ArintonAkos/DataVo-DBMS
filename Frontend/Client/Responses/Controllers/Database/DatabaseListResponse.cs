using Frontend.Client.Responses.Parts;
using Newtonsoft.Json;

namespace Frontend.Client.Responses.Controllers.DatabaseListResponse
{
    public class DatabaseListResponse : Response
    {
        [JsonProperty("data")] public new List<DatabaseModel> Data { get; set; } = new List<DatabaseModel>();
    }
}
