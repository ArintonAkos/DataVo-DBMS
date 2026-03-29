namespace DataVo.Core.Exceptions;

/// <summary>
/// Raised when index lookup, creation, or persistence operations fail.
/// </summary>
public class IndexException : DataVoException
{
    /// <summary>
    /// Initializes an index exception with a message.
    /// </summary>
    /// <param name="message">The failure message.</param>
    public IndexException(string message) : base(message)
    {
    }

    /// <summary>
    /// Initializes an index exception with a message and inner exception.
    /// </summary>
    /// <param name="message">The failure message.</param>
    /// <param name="innerException">The underlying cause.</param>
    public IndexException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
