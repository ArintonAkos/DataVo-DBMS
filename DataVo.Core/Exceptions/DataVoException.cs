namespace DataVo.Core.Exceptions;

/// <summary>
/// Base exception for all DataVo engine errors.
/// Derive from this class to create domain-specific exceptions that callers can distinguish from
/// unrelated system failures.
/// </summary>
/// <example>
/// <code>
/// try { engine.Execute("SELECT ..."); }
/// catch (DataVoException ex) { /* handle any engine error */ }
/// </code>
/// </example>
public class DataVoException : Exception
{
    /// <summary>
    /// Initializes a new instance with the specified error message.
    /// </summary>
    /// <param name="message">A description of the error.</param>
    public DataVoException(string message) : base(message) { }

    /// <summary>
    /// Initializes a new instance with the specified error message and inner exception.
    /// </summary>
    /// <param name="message">A description of the error.</param>
    /// <param name="innerException">The exception that caused this error.</param>
    public DataVoException(string message, Exception innerException) : base(message, innerException) { }
}
