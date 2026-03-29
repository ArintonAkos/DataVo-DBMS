namespace DataVo.Core.Exceptions;

/// <summary>
/// Represents an authentication or authorization failure.
/// </summary>
/// <param name="message">The authorization failure details.</param>
public class AuthorizationException(string message) : DataVoException(message)
{
}
