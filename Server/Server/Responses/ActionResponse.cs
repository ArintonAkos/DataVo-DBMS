using Newtonsoft.Json;
using Server.Server.Responses.Parts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Server.Responses
{
    internal class ActionResponse
    {
        [JsonProperty]
        public ActionDataResponse Data { get; set; }

        [JsonProperty]
        public ActionFieldResponse Fields { get; set; }

        [JsonProperty]
        public List<String> Messages { get; set; } = new();

        public static ActionResponse FromRaw(List<String> messages, List<List<DataResponse>> data, List<FieldResponse> fields)
        {
            return new ActionResponse()
            {
                Messages = messages,
                Data = new ActionDataResponse() { Data = data },
                Fields = new ActionFieldResponse() { Fields = fields },
            };
        }

        public static ActionResponse Default()
        {
            return FromRaw(new(), new(), new());
        }

        public static ActionResponse Error(Exception ex)
        {
            return FromRaw(new() { ex.Message }, new(), new());
        }
    }
}
