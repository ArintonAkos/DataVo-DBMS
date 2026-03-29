namespace DataVo.Core.Exceptions;

/// <summary>
/// Thrown when a catalog metadata operation fails due to a schema constraint violation.
/// Examples include referencing a table that does not exist, creating a duplicate table,
/// or modifying a column that is not present in the schema.
/// </summary>
public class CatalogException : DataVoException
{
    /// <summary>
    /// Initializes a new instance with the specified error message.
    /// </summary>
    /// <param name="message">A description of the catalog error.</param>
    public CatalogException(string message) : base(message) { }

    /// <summary>
    /// Initializes a new instance with the specified error message and inner exception.
    /// </summary>
    /// <param name="message">A description of the catalog error.</param>
    /// <param name="innerException">The underlying exception.</param>
    public CatalogException(string message, Exception innerException) : base(message, innerException) { }
}
