namespace DataVo.Core.Runtime.Security;

/// <summary>
/// Represents the identity and role bound to a logical SQL session.
/// </summary>
public sealed class SessionPrincipal(string username, DatabaseRole role, bool isAuthenticated)
{
    /// <summary>
    /// Gets the principal username associated with the session.
    /// </summary>
    public string Username { get; } = username;

    /// <summary>
    /// Gets the primary role inferred or assigned to the principal.
    /// </summary>
    public DatabaseRole Role { get; } = role;

    /// <summary>
    /// Gets a value indicating whether the session is authenticated.
    /// </summary>
    public bool IsAuthenticated { get; } = isAuthenticated;

    /// <summary>
    /// Gets the complete set of effective role names for this principal.
    /// </summary>
    public IReadOnlyCollection<string> EffectiveRoles { get; init; } = [];

    /// <summary>
    /// Gets the complete set of effective permissions for this principal.
    /// </summary>
    public IReadOnlyCollection<DatabasePermission> EffectivePermissions { get; init; } = [];
}
