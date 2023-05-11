using Newtonsoft.Json;
using Server.Server.Responses.Parts;

namespace Server.Server.Responses;

public class ActionResponse
{
    [JsonProperty] public ActionDataResponse Data { get; set; }

    [JsonProperty] public ActionFieldResponse Fields { get; set; }

    [JsonProperty] public List<string> Messages { get; set; } = new();

    public static ActionResponse FromRaw(List<string> messages, List<List<DataResponse>> data,
        List<FieldResponse> fields) =>
        new()
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