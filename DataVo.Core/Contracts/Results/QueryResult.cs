namespace DataVo.Core.Contracts.Results;

/// <summary>
/// Represents the standardized result returned by query execution.
/// </summary>
/// <example>
/// <code>
/// QueryResult result = QueryResult.Success(
///     ["Rows selected: 1"],
///     [new Dictionary&lt;string, object?&gt; { ["Id"] = 1, ["Name"] = "Alice" }],
///     ["Id", "Name"]);
/// </code>
/// </example>
public class QueryResult
{
    /// <summary>
    /// Gets or sets human-readable messages emitted during execution.
    /// </summary>
    public List<string> Messages { get; set; } = [];

    /// <summary>
    /// Gets or sets the tabular payload returned by the command.
    /// </summary>
    public List<Dictionary<string, object?>> Data { get; set; } = [];

    /// <summary>
    /// Gets or sets the ordered field names associated with <see cref="Data"/>.
    /// </summary>
    public List<string> Fields { get; set; } = [];

    /// <summary>
    /// Gets or sets the total execution time measured for the command.
    /// </summary>
    public TimeSpan ExecutionTime { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the result represents an error.
    /// </summary>
    public bool IsError { get; set; }

    /// <summary>
    /// Creates an error result containing a single message.
    /// </summary>
    /// <param name="message">The error message to expose to the caller.</param>
    /// <returns>A failed <see cref="QueryResult"/>.</returns>
    public static QueryResult Error(string message)
    {
        DataVoError error = DataVoErrorClassifier.ClassifyMessage(message);
        return new() { Messages = [error.Message], IsError = true, ErrorCode = error.Code };
    }

    /// <summary>
    /// Builds an error result from an exception: a stable <see cref="ErrorCode"/>, one clean message
    /// (no exception type names or stack trace), and the position of syntax errors.
    /// </summary>
    public static QueryResult FromException(Exception exception)
    {
        DataVoError error = DataVoErrorClassifier.Classify(exception);
        return new()
        {
            Messages = [error.Message],
            IsError = true,
            ErrorCode = error.Code,
            ErrorLine = error.Line,
            ErrorColumn = error.Column,
        };
    }

    /// <summary>
    /// Stable code from <see cref="DataVoErrorCodes"/> when <see cref="IsError"/> is true; otherwise
    /// <see langword="null"/>.
    /// </summary>
    public string? ErrorCode { get; set; }

    /// <summary>1-based line of the offending token for syntax errors; otherwise <see langword="null"/>.</summary>
    public int? ErrorLine { get; set; }

    /// <summary>1-based column of the offending token for syntax errors; otherwise <see langword="null"/>.</summary>
    public int? ErrorColumn { get; set; }

    /// <summary>
    /// Creates a successful result with messages, data rows, and field metadata.
    /// </summary>
    /// <param name="msg">The execution messages.</param>
    /// <param name="data">The result rows.</param>
    /// <param name="fields">The ordered field names.</param>
    /// <returns>A populated successful <see cref="QueryResult"/>.</returns>
    public static QueryResult Success(List<string> msg, List<Dictionary<string, object?>> data, List<string> fields) => new() { Messages = msg, Data = data, Fields = fields };

    /// <summary>
    /// Creates an empty successful result.
    /// </summary>
    /// <returns>A default <see cref="QueryResult"/> instance.</returns>
    public static QueryResult Default() => new();
}
