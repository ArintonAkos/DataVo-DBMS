namespace DataVo.Core.Exceptions;

/// <summary>
/// Raised when index lookup, creation, or persistence operations fail.
/// </summary>
public class IndexException : DataVoException
{
    public IndexException(string message) : base(message)
    {
    }

    public IndexException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
