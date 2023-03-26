using Newtonsoft.Json;
using Frontend.Client.Responses.Parts;
using System.Collections.Generic;
using System;

namespace Frontend.Client.Responses
{
    internal class ActionResponse
    {
        [JsonProperty]
        public ActionDataResponse Data { get; set; }

        [JsonProperty]
        public ActionFieldResponse Fields { get; set; }

        [JsonProperty]
        public List<String> Messages { get; set; } = new List<String>();

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
            return FromRaw(new List<String>(), new List<List<DataResponse>>(), new List<FieldResponse>());
        }

        public static ActionResponse Error(Exception ex)
        {
            return FromRaw(new List<String>() { ex.Message }, new List<List<DataResponse>>(), new List<FieldResponse>());
        }
    }
}
