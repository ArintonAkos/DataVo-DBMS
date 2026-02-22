namespace DataVo.Core.Contracts.Results;

public class QueryResult
{
    public List<string> Messages { get; set; } = [];
    public List<Dictionary<string, dynamic>> Data { get; set; } = [];
    public List<string> Fields { get; set; } = [];
    public TimeSpan ExecutionTime { get; set; }
    public bool IsError { get; set; }

    public static QueryResult Error(string message) => new QueryResult { Messages = [message], IsError = true };
    public static QueryResult Success(List<string> msg, List<Dictionary<string, dynamic>> data, List<string> fields) => new QueryResult { Messages = msg, Data = data, Fields = fields };
    public static QueryResult Default() => new QueryResult();
}
