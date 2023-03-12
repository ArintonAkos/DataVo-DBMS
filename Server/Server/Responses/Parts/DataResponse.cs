using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Server.Responses.Parts
{
    internal class DataResponse
    {
        [JsonProperty]
        public string Value { get; set; }

        [JsonProperty]
        public string FieldName { get; set; }
    }
}
