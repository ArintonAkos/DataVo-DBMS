using System.Collections.Concurrent;
using DataVo.Core.Exceptions;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Core.Runtime.Security;

/// <summary>
/// Stores and validates session principals for authentication/authorization.
/// </summary>
internal sealed class SessionSecurityStore
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

        AuthUser? user;
        HashSet<DatabasePermission> effectivePermissions;
        HashSet<string> effectiveRoles;

        lock (_sync)
        {
            if (!_users.TryGetValue(username, out user))
            {
                return false;
            }

            if (!PasswordHasher.Verify(password, user.PasswordHash, user.PasswordSalt))
            {
                return false;
            }

            effectivePermissions = ResolvePermissions(user);
            effectiveRoles = [.. user.Roles];
        }

        Set(session, new SessionPrincipal(user.Username, InferPrimaryRole(effectivePermissions), isAuthenticated: true)
        {
            EffectivePermissions = effectivePermissions,
            EffectiveRoles = effectiveRoles
        });
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
            UpsertConfigUser(config, user);
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

            DataVoAuthUser? configUser = config.AuthorizationUsers
                .FirstOrDefault(candidate => string.Equals(candidate.Username, username, StringComparison.OrdinalIgnoreCase));
            if (configUser != null)
            {
                config.AuthorizationUsers.Remove(configUser);
            }
        }
    }

    public void CreateRole(string roleName)
    {
        lock (_sync)
        {
            if (_roles.ContainsKey(roleName))
            {
                throw new AuthorizationException($"Role '{roleName}' already exists.");
            }

            _roles[roleName] = [];
        }
    }

    public void DropRole(string roleName)
    {
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
        }
    }

    public void GrantPermissionsToUser(string username, IEnumerable<DatabasePermission> permissions)
    {
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
        }
    }

    public void RevokePermissionsFromUser(string username, IEnumerable<DatabasePermission> permissions)
    {
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
        }
    }

    public void GrantRoleToUser(string roleName, string username)
    {
        lock (_sync)
        {
            EnsureRoleExists(roleName);

            if (!_users.TryGetValue(username, out AuthUser? user))
            {
                throw new AuthorizationException($"User '{username}' does not exist.");
            }

            user.Roles.Add(roleName);
        }
    }

    public void RevokeRoleFromUser(string roleName, string username)
    {
        lock (_sync)
        {
            if (!_users.TryGetValue(username, out AuthUser? user))
            {
                throw new AuthorizationException($"User '{username}' does not exist.");
            }

            user.Roles.Remove(roleName);
        }
    }

    public void GrantPermissionsToRole(string roleName, IEnumerable<DatabasePermission> permissions)
    {
        lock (_sync)
        {
            EnsureRoleExists(roleName);

            HashSet<DatabasePermission> rolePermissions = _roles[roleName];
            foreach (DatabasePermission permission in permissions)
            {
                rolePermissions.Add(permission);
            }
        }
    }

    public void RevokePermissionsFromRole(string roleName, IEnumerable<DatabasePermission> permissions)
    {
        lock (_sync)
        {
            EnsureRoleExists(roleName);

            HashSet<DatabasePermission> rolePermissions = _roles[roleName];
            foreach (DatabasePermission permission in permissions)
            {
                rolePermissions.Remove(permission);
            }
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

    private static bool HasPermission(SessionPrincipal principal, DatabasePermission permission)
    {
        if (principal.EffectivePermissions.Contains(DatabasePermission.Admin)
            || principal.EffectivePermissions.Contains(permission))
        {
            return true;
        }

        return principal.Role switch
        {
            DatabaseRole.Admin => true,
            DatabaseRole.ReadWrite => permission is DatabasePermission.ReadData
                or DatabasePermission.WriteData
                or DatabasePermission.ManageTransactions,
            DatabaseRole.ReadOnly => permission is DatabasePermission.ReadData,
            _ => false
        };
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
            SeedBuiltInRole(DatabaseRole.Admin.ToString(),
#if NET6_0_OR_GREATER
                Enum.GetValues<DatabasePermission>());
#else
                Enum.GetValues(typeof(DatabasePermission)).Cast<DatabasePermission>());
#endif

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

    private static DatabaseRole InferPrimaryRole(HashSet<DatabasePermission> permissions)
    {
        if (permissions.Contains(DatabasePermission.Admin)
            || permissions.Contains(DatabasePermission.ManageSecurity)
            || permissions.Contains(DatabasePermission.ManageSchema))
        {
            return DatabaseRole.Admin;
        }

        if (permissions.Contains(DatabasePermission.WriteData))
        {
            return DatabaseRole.ReadWrite;
        }

        return DatabaseRole.ReadOnly;
    }

    private void UpsertConfigUser(DataVoConfig config, AuthUser user)
    {
        DataVoAuthUser? existing = config.AuthorizationUsers
            .FirstOrDefault(candidate => string.Equals(candidate.Username, user.Username, StringComparison.OrdinalIgnoreCase));

        if (existing == null)
        {
            existing = new DataVoAuthUser { Username = user.Username };
            config.AuthorizationUsers.Add(existing);
        }

        existing.Password = string.Empty;
        existing.PasswordHash = user.PasswordHash;
        existing.PasswordSalt = user.PasswordSalt;
        existing.Role = InferPrimaryRole(ResolvePermissions(user));
        existing.Roles = [.. user.Roles.OrderBy(value => value, StringComparer.OrdinalIgnoreCase)];
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
