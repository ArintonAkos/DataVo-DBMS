namespace DataVo.Core.Exceptions;

/// <summary>
/// Represents an error raised while tokenizing raw SQL text.
/// </summary>
/// <param name="message">The lexer failure details.</param>
public class LexerException(string message) : Exception(message)
{
}
