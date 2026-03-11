namespace DataVo.Core.Exceptions;

/// <summary>
/// Represents an operation that expected a source file path but received none.
/// </summary>
internal class NoSourceFileProvided : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="NoSourceFileProvided"/> class.
    /// </summary>
    public NoSourceFileProvided()
        : base("No source file was found when calling the compiler!")
    {
    }
}