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
}
