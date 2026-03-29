namespace DataVo.Core.Exceptions;

/// <summary>
/// Represents a runtime evaluation error that occurs after parsing and binding.
/// </summary>
/// <param name="message">The evaluation failure details.</param>
public class EvaluationException(string message) : DataVoException(message)
{
}
