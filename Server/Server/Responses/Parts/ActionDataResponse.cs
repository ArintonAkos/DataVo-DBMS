using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Server.Responses.Parts
{
    internal class ActionDataResponse
    {
        [JsonProperty]
        public List<List<DataResponse>> Data { get; set; } = new();
    }
}
