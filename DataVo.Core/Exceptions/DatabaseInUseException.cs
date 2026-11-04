namespace DataVo.Core.Exceptions;

/// <summary>
/// Thrown when a data directory cannot be opened because another process holds it, or because this
/// process already has it open with a different, incompatible configuration.
/// </summary>
/// <example>
/// <code>
/// try { using var db = new DataVoContext(config); }
/// catch (DatabaseInUseException ex) { logger.Error(ex.Message); }
/// </code>
/// </example>
public class DatabaseInUseException : StorageException
{
    /// <summary>
    /// Initializes a new instance for the given directory.
    /// </summary>
    /// <param name="directory">The data directory that could not be opened.</param>
    /// <param name="message">A description of why the directory is unavailable.</param>
    /// <param name="innerException">The underlying failure, when there is one.</param>
    public DatabaseInUseException(string directory, string message, Exception? innerException = null)
        : base(message, innerException ?? new InvalidOperationException(message))
    {
        Directory = directory;
    }

    /// <summary>
    /// Gets the data directory that could not be opened.
    /// </summary>
    public string Directory { get; }
}
