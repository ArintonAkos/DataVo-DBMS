namespace DataVo.Core.Runtime.Security;

/// <summary>
/// Defines built-in runtime roles used by engine authorization.
/// </summary>
public enum DatabaseRole
{
    /// <summary>
    /// Allows read-only operations.
    /// </summary>
    ReadOnly,

    /// <summary>
    /// Allows read and write data operations.
    /// </summary>
    ReadWrite,

    /// <summary>
    /// Allows all operations, including schema and security management.
    /// </summary>
    Admin
}
