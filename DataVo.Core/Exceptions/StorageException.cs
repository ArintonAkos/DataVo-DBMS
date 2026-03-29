namespace DataVo.Core.Exceptions;

/// <summary>
/// Thrown when a storage-level I/O operation fails.
/// Wraps file read/write failures, corruption, and permission errors so the caller can
/// distinguish infrastructure problems from logical query errors.
/// </summary>
/// <example>
/// <code>
/// try { storageEngine.ReadRow("db", "users", 42); }
/// catch (StorageException ex) { logger.Error($"Disk I/O failure: {ex.Message}"); }
/// </code>
/// </example>
public class StorageException : DataVoException
{
    /// <summary>
    /// Initializes a new instance with the specified error message.
    /// </summary>
    /// <param name="message">A description of the storage failure.</param>
    public StorageException(string message) : base(message) { }

    /// <summary>
    /// Initializes a new instance with the specified error message and inner exception.
    /// </summary>
    /// <param name="message">A description of the storage failure.</param>
    /// <param name="innerException">The underlying I/O exception.</param>
    public StorageException(string message, Exception innerException) : base(message, innerException) { }
}
