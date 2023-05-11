using Frontend.Client.Responses;
using Newtonsoft.Json;
using Server.Server.Responses.Parts;
using System.Collections.Generic;

namespace Frontend.Client.Responses.Controllers.DatabaseListResponse
{
    public class DatabaseListResponse : Response
    {
        [JsonProperty("data")] public new List<DatabaseModel> Data { get; set; } = new List<DatabaseModel>();
    }
}
