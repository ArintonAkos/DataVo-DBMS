using Newtonsoft.Json;
using Server.Server.Responses.Parts;

namespace Server.Server.Responses;

internal class ActionResponse
{
    [JsonProperty] public ActionDataResponse Data { get; set; }

    [JsonProperty] public ActionFieldResponse Fields { get; set; }

    [JsonProperty] public List<string> Messages { get; set; } = new();

    public static ActionResponse FromRaw(List<string> messages, List<List<DataResponse>> data,
        List<FieldResponse> fields)
    {
        return new ActionResponse
        {
            Messages = messages,
            Data = new ActionDataResponse { Data = data },
            Fields = new ActionFieldResponse { Fields = fields }
        };
    }

    public static ActionResponse Default()
    {
        return FromRaw(new List<string>(), new List<List<DataResponse>>(), new List<FieldResponse>());
    }

    public static ActionResponse Error(Exception ex)
    {
        return FromRaw(new List<string> { ex.Message }, new List<List<DataResponse>>(), new List<FieldResponse>());
    }
}