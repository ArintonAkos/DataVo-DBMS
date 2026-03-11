namespace DataVo.Core.Exceptions;

/// <summary>
/// Represents an error raised while constructing the SQL abstract syntax tree.
/// </summary>
/// <param name="message">The parser failure details.</param>
public class ParserException(string message) : Exception(message)
{
}
