namespace DataVo.Core.Exceptions;

/// <summary>
/// Thrown when a row violates a constraint (primary key, UNIQUE, foreign key, NOT NULL) or a column's
/// type or length. The whole statement is rejected and nothing is written;
/// <see cref="DataVoException.ErrorCode"/> names the first violation.
/// </summary>
/// <example>
/// <code>
/// try { db.InsertTyped("Players", schema, row); }
/// catch (ConstraintViolationException ex) when (ex.ErrorCode == DataVoErrorCodes.PrimaryKeyViolation) { ... }
/// </code>
/// </example>
public class ConstraintViolationException : DataVoException
{
    /// <summary>Initializes a new instance with the specified message.</summary>
    public ConstraintViolationException(string message) : base(message) { }
}
