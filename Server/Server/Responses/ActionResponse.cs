using Newtonsoft.Json;
using DataVo.Core.Contracts.Results;

namespace Server.Server.Responses;

public class ActionResponse
{
    [JsonProperty] public List<Dictionary<string, dynamic>> Data { get; set; } = new();

    [JsonProperty] public List<string> Fields { get; set; } = new();

    [JsonProperty] public List<string> Messages { get; set; } = new();
    
    [JsonProperty] public bool IsError { get; set; }

    public static ActionResponse FromQueryResult(QueryResult result) =>
        new()
        {
            Messages = result.Messages,
            Data = result.Data,
            Fields = result.Fields,
            IsError = result.IsError
        };

    public static ActionResponse Default() =>
        new();

    public static ActionResponse Error(Exception ex) => new() { Messages = new List<string> { ex.Message }, IsError = true };
}
