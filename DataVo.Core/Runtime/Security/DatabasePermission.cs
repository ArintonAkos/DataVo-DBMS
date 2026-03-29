namespace DataVo.Core.Runtime.Security;

/// <summary>
/// Represents broad capability buckets used by runtime authorization checks.
/// </summary>
public enum DatabasePermission
{
    /// <summary>
    /// Allows operations needed to establish or clear session identity.
    /// </summary>
    Authenticate,

    /// <summary>
    /// Allows read/query operations.
    /// </summary>
    ReadData,

    /// <summary>
    /// Allows insert/update/delete operations.
    /// </summary>
    WriteData,

    /// <summary>
    /// Allows schema/data-definition operations.
    /// </summary>
    ManageSchema,

    /// <summary>
    /// Allows transaction control operations.
    /// </summary>
    ManageTransactions,

    /// <summary>
    /// Allows user/role/grant management operations.
    /// </summary>
    ManageSecurity,

    /// <summary>
    /// Grants all permissions.
    /// </summary>
    Admin
}
