namespace DataVo.Data;

/// <summary>
/// Represents errors returned by the DataVo engine during SQL execution.
/// </summary>
public class DataVoException : Exception
{
    /// <summary>
    /// Creates a new <see cref="DataVoException"/> with the specified message.
    /// </summary>
    public DataVoException(string message) : base(message) { }

    /// <summary>
    /// Creates a new <see cref="DataVoException"/> wrapping an inner exception.
    /// </summary>
    public DataVoException(string message, Exception inner) : base(message, inner) { }

    /// <summary>
    /// The engine's stable error code (see <c>DataVo.Core.Contracts.Results.DataVoErrorCodes</c>), for
    /// example <c>DV3001_PRIMARY_KEY_VIOLATION</c>.
    /// </summary>
    public string? ErrorCode { get; init; }

    /// <summary>1-based line of the offending SQL token, for syntax errors.</summary>
    public int? Line { get; init; }

    /// <summary>1-based column of the offending SQL token, for syntax errors.</summary>
    public int? Column { get; init; }
}
