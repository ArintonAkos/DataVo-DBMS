namespace DataVo.Core.Exceptions;

/// <summary>
/// Represents a failure to bind parsed identifiers to concrete schema objects.
/// </summary>
/// <param name="message">The binding failure details.</param>
internal class BindingException(string message) : Exception(message)
{
}