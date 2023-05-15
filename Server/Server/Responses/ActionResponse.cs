using Newtonsoft.Json;
using Server.Server.Responses.Parts;

namespace Server.Server.Responses;

public class ActionResponse
{
    [JsonProperty] public List<List<dynamic>> Data { get; set; }

    [JsonProperty] public List<FieldResponse> Fields { get; set; }

    [JsonProperty] public List<string> Messages { get; set; } = new();

    public static ActionResponse FromRaw(List<string> messages, List<List<dynamic>> data,
        List<FieldResponse> fields) =>
        new()
        {
            Messages = messages,
            Data = data,
            Fields = fields,
        };

    public static ActionResponse Default() =>
        FromRaw(new(), new(), new());

    public static ActionResponse Error(Exception ex) => FromRaw(new List<string> { ex.Message, },
        new(), new());
}