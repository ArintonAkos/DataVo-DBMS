using System.Collections.Concurrent;
using DataVo.Core.Exceptions;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Core.Runtime.Security;

/// <summary>
/// Stores and validates session principals for authentication/authorization.
/// </summary>
internal sealed partial class SessionSecurityStore
{
    private sealed class AuthUser
    {
        public required string Username { get; init; }
        public string PasswordHash { get; set; } = string.Empty;
        public string PasswordSalt { get; set; } = string.Empty;
        public HashSet<string> Roles { get; } = new(StringComparer.OrdinalIgnoreCase);
        public HashSet<DatabasePermission> Permissions { get; } = [];
    }

    private readonly ConcurrentDictionary<Guid, SessionPrincipal> _sessionPrincipals = new();
    private readonly Dictionary<string, AuthUser> _users = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, HashSet<DatabasePermission>> _roles = new(StringComparer.OrdinalIgnoreCase);
    private readonly object _sync = new();
    private bool _initialized;
    private static readonly AsyncLocal<SessionPrincipal?> AmbientPrincipal = new();

    public SessionPrincipal? Get(Guid session)
    {
        return _sessionPrincipals.GetValueOrDefault(session);
    }

    public void Set(Guid session, SessionPrincipal principal)
    {
        _sessionPrincipals[session] = principal;
    }

    public void Remove(Guid session)
    {
        _sessionPrincipals.TryRemove(session, out _);
    }

    public SessionPrincipal? GetAmbientPrincipal()
    {
        return AmbientPrincipal.Value;
    }

    public IDisposable PushAmbientPrincipalForSession(Guid session)
    {
        return PushAmbientPrincipal(Get(session));
    }

    public IDisposable PushAmbientPrincipal(SessionPrincipal? principal)
    {
        SessionPrincipal? previous = AmbientPrincipal.Value;
        AmbientPrincipal.Value = principal;

        return new Scope(() => AmbientPrincipal.Value = previous);
    }

    public bool Authenticate(Guid session, DataVoConfig config, string username, string password)
    {
        EnsureInitialized(config);

        lock (_sync)
        {
            if (!_users.TryGetValue(username, out AuthUser? user))
            {
                return false;
            }

            if (!PasswordHasher.Verify(password, user.PasswordHash, user.PasswordSalt))
            {
                return false;
            }

            Set(session, BuildPrincipal(user));
        }

        return true;
    }

    public void CreateUser(DataVoConfig config, string username, string password, string? roleName)
    {
        EnsureInitialized(config);

        lock (_sync)
        {
            if (_users.ContainsKey(username))
            {
                throw new AuthorizationException($"User '{username}' already exists.");
            }

            var (hash, salt) = PasswordHasher.HashPassword(password);
            var user = new AuthUser
            {
                Username = username,
                PasswordHash = hash,
                PasswordSalt = salt
            };

            if (!string.IsNullOrWhiteSpace(roleName))
            {
                EnsureRoleExists(roleName);
                user.Roles.Add(roleName);
            }

            _users[user.Username] = user;
        }
    }

    public void DropUser(DataVoConfig config, string username)
    {
        EnsureInitialized(config);

        lock (_sync)
        {
            if (!_users.Remove(username))
            {
                throw new AuthorizationException($"User '{username}' does not exist.");
            }

            RefreshSessionsLocked();
        }
    }

    public void CreateRole(DataVoConfig config, string roleName)
    {
        EnsureInitialized(config);

        lock (_sync)
        {
            if (_roles.ContainsKey(roleName))
            {
                throw new AuthorizationException($"Role '{roleName}' already exists.");
            }

            _roles[roleName] = [];
        }
    }

    public void DropRole(DataVoConfig config, string roleName)
    {
        EnsureInitialized(config);

        lock (_sync)
        {
            if (IsBuiltInRole(roleName))
            {
                throw new AuthorizationException($"Role '{roleName}' is built-in and cannot be dropped.");
            }

            if (!_roles.Remove(roleName))
            {
                throw new AuthorizationException($"Role '{roleName}' does not exist.");
            }

            foreach (AuthUser user in _users.Values)
            {
                user.Roles.Remove(roleName);
            }

            RefreshSessionsLocked();
        }
    }

    public void GrantPermissionsToUser(DataVoConfig config, string username, IEnumerable<DatabasePermission> permissions)
    {
        EnsureInitialized(config);

        lock (_sync)
        {
            if (!_users.TryGetValue(username, out AuthUser? user))
            {
                throw new AuthorizationException($"User '{username}' does not exist.");
            }

            foreach (DatabasePermission permission in permissions)
            {
                user.Permissions.Add(permission);
            }

            RefreshSessionsLocked();
        }
    }

    public void RevokePermissionsFromUser(DataVoConfig config, string username, IEnumerable<DatabasePermission> permissions)
    {
        EnsureInitialized(config);

        lock (_sync)
        {
            if (!_users.TryGetValue(username, out AuthUser? user))
            {
                throw new AuthorizationException($"User '{username}' does not exist.");
            }

            foreach (DatabasePermission permission in permissions)
            {
                user.Permissions.Remove(permission);
            }

            RefreshSessionsLocked();
        }
    }

    public void GrantRoleToUser(DataVoConfig config, string roleName, string username)
    {
        EnsureInitialized(config);

        lock (_sync)
        {
            EnsureRoleExists(roleName);

            if (!_users.TryGetValue(username, out AuthUser? user))
            {
                throw new AuthorizationException($"User '{username}' does not exist.");
            }

            user.Roles.Add(roleName);

            RefreshSessionsLocked();
        }
    }

    public void RevokeRoleFromUser(DataVoConfig config, string roleName, string username)
    {
        EnsureInitialized(config);

        lock (_sync)
        {
            if (!_users.TryGetValue(username, out AuthUser? user))
            {
                throw new AuthorizationException($"User '{username}' does not exist.");
            }

            user.Roles.Remove(roleName);

            RefreshSessionsLocked();
        }
    }

    public void GrantPermissionsToRole(DataVoConfig config, string roleName, IEnumerable<DatabasePermission> permissions)
    {
        EnsureInitialized(config);

        lock (_sync)
        {
            EnsureRoleExists(roleName);

            HashSet<DatabasePermission> rolePermissions = _roles[roleName];
            foreach (DatabasePermission permission in permissions)
            {
                rolePermissions.Add(permission);
            }

            RefreshSessionsLocked();
        }
    }

    public void RevokePermissionsFromRole(DataVoConfig config, string roleName, IEnumerable<DatabasePermission> permissions)
    {
        EnsureInitialized(config);

        lock (_sync)
        {
            EnsureRoleExists(roleName);

            HashSet<DatabasePermission> rolePermissions = _roles[roleName];
            foreach (DatabasePermission permission in permissions)
            {
                rolePermissions.Remove(permission);
            }

            RefreshSessionsLocked();
        }
    }

    public IReadOnlyList<SecurityUserView> ListUsers(DataVoConfig config)
    {
        EnsureInitialized(config);

        lock (_sync)
        {
            return _users.Values
                .OrderBy(user => user.Username, StringComparer.OrdinalIgnoreCase)
                .Select(user => new SecurityUserView(
                    user.Username,
                    user.Roles.OrderBy(role => role, StringComparer.OrdinalIgnoreCase).ToArray(),
                    user.Permissions.OrderBy(permission => permission.ToString(), StringComparer.Ordinal).ToArray()))
                .ToArray();
        }
    }

    public IReadOnlyList<SecurityRoleView> ListRoles(DataVoConfig config)
    {
        EnsureInitialized(config);

        lock (_sync)
        {
            return _roles
                .OrderBy(entry => entry.Key, StringComparer.OrdinalIgnoreCase)
                .Select(entry => new SecurityRoleView(
                    entry.Key,
                    entry.Value.OrderBy(permission => permission.ToString(), StringComparer.Ordinal).ToArray()))
                .ToArray();
        }
    }

    public IReadOnlyList<SecurityGrantView> ListGrants(DataVoConfig config)
    {
        EnsureInitialized(config);

        lock (_sync)
        {
            var grants = new List<SecurityGrantView>();

            foreach (AuthUser user in _users.Values)
            {
                foreach (string role in user.Roles.OrderBy(value => value, StringComparer.OrdinalIgnoreCase))
                {
                    grants.Add(new SecurityGrantView("USER", user.Username, "ROLE", role));
                }

                foreach (DatabasePermission permission in user.Permissions.OrderBy(value => value.ToString(), StringComparer.Ordinal))
                {
                    grants.Add(new SecurityGrantView("USER", user.Username, "PERMISSION", permission.ToString()));
                }
            }

            foreach ((string roleName, HashSet<DatabasePermission> permissions) in _roles)
            {
                foreach (DatabasePermission permission in permissions.OrderBy(value => value.ToString(), StringComparer.Ordinal))
                {
                    grants.Add(new SecurityGrantView("ROLE", roleName, "PERMISSION", permission.ToString()));
                }
            }

            return grants
                .OrderBy(grant => grant.GranteeType, StringComparer.Ordinal)
                .ThenBy(grant => grant.GranteeName, StringComparer.OrdinalIgnoreCase)
                .ThenBy(grant => grant.GrantType, StringComparer.Ordinal)
                .ThenBy(grant => grant.GrantValue, StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }
    }

    public void Authorize(Guid session, DatabasePermission permission, DataVoConfig config)
    {
        if (permission == DatabasePermission.Authenticate)
        {
            return;
        }

        EnsureInitialized(config);

        if (!config.EnableAuthorization)
        {
            return;
        }

        SessionPrincipal? principal = Get(session) ?? AmbientPrincipal.Value;
        if (principal == null)
        {
            if (!config.AllowAnonymousSession)
            {
                throw new AuthorizationException("Authentication is required for this operation.");
            }

            principal = BuildAnonymousPrincipal(config);
        }

        if (!HasPermission(principal, permission))
        {
            throw new AuthorizationException(
                $"Session principal '{principal.Username}' with role '{principal.Role}' is not authorized for '{permission}'.");
        }
    }

    private SessionPrincipal BuildAnonymousPrincipal(DataVoConfig config)
    {
        string roleName = config.AnonymousRole.ToString();
        HashSet<DatabasePermission> permissions;
        lock (_sync)
        {
            EnsureRoleExists(roleName);
            permissions = [.. _roles[roleName]];
        }

        return new SessionPrincipal("anonymous", config.AnonymousRole, isAuthenticated: false)
        {
            EffectiveRoles = [roleName],
            EffectivePermissions = permissions
        };
    }

    /// <summary>
    /// Checks a permission against the principal's resolved grants only; the inferred primary role is
    /// informational and never widens what the grants allow.
    /// </summary>
    private static bool HasPermission(SessionPrincipal principal, DatabasePermission permission)
    {
        return principal.EffectivePermissions.Contains(DatabasePermission.Admin)
            || principal.EffectivePermissions.Contains(permission);
    }

    private void EnsureInitialized(DataVoConfig config)
    {
        if (_initialized)
        {
            return;
        }

        lock (_sync)
        {
            if (_initialized)
            {
                return;
            }

            SeedBuiltInRole(DatabaseRole.ReadOnly.ToString(), [DatabasePermission.ReadData]);
            SeedBuiltInRole(DatabaseRole.ReadWrite.ToString(), [
                DatabasePermission.ReadData,
                DatabasePermission.WriteData,
                DatabasePermission.ManageTransactions
            ]);
            SeedBuiltInRole(DatabaseRole.Admin.ToString(), GetDatabasePermissions());

            foreach (DataVoAuthUser configUser in config.AuthorizationUsers)
            {
                if (string.IsNullOrWhiteSpace(configUser.Username))
                {
                    continue;
                }

                string hash = configUser.PasswordHash;
                string salt = configUser.PasswordSalt;
                if (string.IsNullOrWhiteSpace(hash) || string.IsNullOrWhiteSpace(salt))
                {
                    if (string.IsNullOrWhiteSpace(configUser.Password))
                    {
                        continue;
                    }

                    (hash, salt) = PasswordHasher.HashPassword(configUser.Password);
                    configUser.PasswordHash = hash;
                    configUser.PasswordSalt = salt;
                    configUser.Password = string.Empty;
                }

                var user = new AuthUser
                {
                    Username = configUser.Username,
                    PasswordHash = hash,
                    PasswordSalt = salt
                };

                user.Roles.Add(configUser.Role.ToString());
                foreach (string role in configUser.Roles)
                {
                    if (!string.IsNullOrWhiteSpace(role))
                    {
                        EnsureRoleExists(role);
                        user.Roles.Add(role);
                    }
                }

                _users[user.Username] = user;
            }

            _initialized = true;
        }
    }

    private void SeedBuiltInRole(string roleName, IEnumerable<DatabasePermission> permissions)
    {
        _roles[roleName] = new HashSet<DatabasePermission>(permissions);
    }

    private void EnsureRoleExists(string roleName)
    {
        if (!_roles.ContainsKey(roleName))
        {
            throw new AuthorizationException($"Role '{roleName}' does not exist.");
        }
    }

    private static bool IsBuiltInRole(string roleName)
    {
        return roleName.Equals(DatabaseRole.ReadOnly.ToString(), StringComparison.OrdinalIgnoreCase)
            || roleName.Equals(DatabaseRole.ReadWrite.ToString(), StringComparison.OrdinalIgnoreCase)
            || roleName.Equals(DatabaseRole.Admin.ToString(), StringComparison.OrdinalIgnoreCase);
    }

    private HashSet<DatabasePermission> ResolvePermissions(AuthUser user)
    {
        HashSet<DatabasePermission> resolved = [.. user.Permissions];
        foreach (string role in user.Roles)
        {
            if (_roles.TryGetValue(role, out HashSet<DatabasePermission>? rolePermissions))
            {
                resolved.UnionWith(rolePermissions);
            }
        }

        return resolved;
    }

    /// <summary>
    /// Infers the display role of a grant set: <see cref="DatabaseRole.Admin"/> only for the explicit
    /// <see cref="DatabasePermission.Admin"/> grant, so schema or security rights alone never imply admin.
    /// </summary>
    private static DatabaseRole InferPrimaryRole(HashSet<DatabasePermission> permissions)
    {
        if (permissions.Contains(DatabasePermission.Admin))
        {
            return DatabaseRole.Admin;
        }

        if (permissions.Contains(DatabasePermission.WriteData))
        {
            return DatabaseRole.ReadWrite;
        }

        return DatabaseRole.ReadOnly;
    }

    /// <summary>Builds an authenticated principal from the user's current grants. Caller holds <c>_sync</c>.</summary>
    private SessionPrincipal BuildPrincipal(AuthUser user)
    {
        HashSet<DatabasePermission> permissions = ResolvePermissions(user);
        return new SessionPrincipal(user.Username, InferPrimaryRole(permissions), isAuthenticated: true)
        {
            EffectivePermissions = permissions,
            EffectiveRoles = [.. user.Roles]
        };
    }

    /// <summary>
    /// Re-resolves every authenticated session after a security change so revoked rights stop applying
    /// immediately, and logs out sessions whose user was dropped. Caller holds <c>_sync</c>.
    /// </summary>
    private void RefreshSessionsLocked()
    {
        foreach (KeyValuePair<Guid, SessionPrincipal> entry in _sessionPrincipals)
        {
            if (!entry.Value.IsAuthenticated)
            {
                continue;
            }

            if (_users.TryGetValue(entry.Value.Username, out AuthUser? user))
            {
                _sessionPrincipals[entry.Key] = BuildPrincipal(user);
            }
            else
            {
                _sessionPrincipals.TryRemove(entry.Key, out _);
            }
        }
    }

    private sealed class Scope(Action onDispose) : IDisposable
    {
        private readonly Action _onDispose = onDispose;
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _onDispose();
            _disposed = true;
        }
    }
}
