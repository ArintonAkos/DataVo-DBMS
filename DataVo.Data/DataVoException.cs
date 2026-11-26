using System.Data.Common;
using DataVo.Core.Contracts.Results;

namespace DataVo.Data;

/// <summary>
/// Represents errors returned by the DataVo engine during SQL execution. Derives from <see cref="DbException"/> so
/// provider-agnostic code (and libraries such as Dapper) can catch it as a database error.
/// </summary>
public class DataVoException : DbException
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
    /// example <c>DV3001_PRIMARY_KEY_VIOLATION</c>. It hides <see cref="System.Runtime.InteropServices.ExternalException.ErrorCode"/>,
    /// the numeric HRESULT that DataVo does not use.
    /// </summary>
    public new string? ErrorCode { get; init; }

    /// <summary>1-based line of the offending SQL token, for syntax errors.</summary>
    public int? Line { get; init; }

    /// <summary>1-based column of the offending SQL token, for syntax errors.</summary>
    public int? Column { get; init; }

    /// <summary>
    /// Builds the exception for a failed engine result, carrying its messages, error code and source position.
    /// </summary>
    /// <param name="result">The failed result.</param>
    /// <returns>The exception to throw.</returns>
    internal static DataVoException FromResult(QueryResult result) =>
        new(string.Join("; ", result.Messages))
        {
            ErrorCode = result.ErrorCode,
            Line = result.ErrorLine,
            Column = result.ErrorColumn,
        };
}
