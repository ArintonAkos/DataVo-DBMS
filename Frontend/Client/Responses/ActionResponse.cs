using System;
using System.Collections.Generic;
using Frontend.Client.Responses.Parts;
using Newtonsoft.Json;

namespace Frontend.Client.Responses
{
    public class ActionResponse
    {
        [JsonProperty] public ActionDataResponse Data { get; set; }

        [JsonProperty] public ActionFieldResponse Fields { get; set; }

        [JsonProperty] public List<string> Messages { get; set; } = new List<string>();

        public static ActionResponse FromRaw(List<string> messages, List<List<DataResponse>> data,
            List<FieldResponse> fields) =>
            new ActionResponse
            {
                Messages = messages,
                Data = new ActionDataResponse { Data = data, },
                Fields = new ActionFieldResponse { Fields = fields, },
            };

        public static ActionResponse Default() =>
            FromRaw(new List<string>(), new List<List<DataResponse>>(), new List<FieldResponse>());

        public static ActionResponse Error(Exception ex) => FromRaw(new List<string> { ex.Message, },
            new List<List<DataResponse>>(), new List<FieldResponse>());
    }
}