using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.Runtime.Security;
using DataVo.Core.Transactions;
using DataVo.Core.MVCC;
using PolyIndexManager = DataVo.Core.Indexing.IndexManager;

namespace DataVo.Core.Runtime;

/// <summary>
/// Represents the runtime engine context used by query execution.
/// </summary>
/// <remarks>
/// This is the first step toward an instance-scoped architecture. The class currently wraps
/// the active <see cref="StorageContext"/> and configuration so callers can pass an explicit
/// engine context into the parser pipeline instead of relying directly on singleton access.
/// </remarks>
/// <example>
/// <code>
/// DataVoEngine engine = DataVoEngine.Initialize(config);
/// var queryEngine = new QueryEngine("SELECT * FROM Users;", sessionId, engine);
/// var results = queryEngine.Parse();
/// </code>
/// </example>
public sealed class DataVoEngine : IDisposable
{
    private static readonly AsyncLocal<DataVoEngine?> ScopedCurrent = new();
    private static readonly object SyncRoot = new();
    private static DataVoEngine? _fallbackCurrent;
    private readonly TransactionIdStateStore? _transactionIdStateStore;

    private DataVoEngine(StorageContext storageContext)
    {
        Id = Guid.NewGuid();
        StorageContext = storageContext;
        Config = storageContext.Config;
        Sessions = new SessionDatabaseStore();
        SessionSecurity = new SessionSecurityStore();
        Catalog = new EngineCatalog(Config);
        StorageContext.AttachRuntimeCatalog(Catalog, Id);
        TransactionManager = new TransactionManager();
        LockManager = new LockManager(Config.LockAcquireTimeoutMs);
        IndexManager = new PolyIndexManager(Config, ResolveIndexRootDirectory());
        VersionStorageManager = new VersionStorageManager();
        TransactionIdAllocator = new TransactionIdAllocator();

        if (Config.StorageMode == StorageMode.Disk)
        {
            _transactionIdStateStore = new TransactionIdStateStore(Config);
            long? persistedHighWaterMark = _transactionIdStateStore.TryReadHighWaterMark();
            if (persistedHighWaterMark.HasValue)
            {
                TransactionIdAllocator.RestoreHighWaterMark(persistedHighWaterMark.Value + 1);
            }

            TransactionIdAllocator.SetHighWaterMarkObserver(highWaterMark =>
                _transactionIdStateStore.PersistHighWaterMark(highWaterMark));
        }
    }

    /// <summary>
    /// Gets the unique engine instance identifier.
    /// </summary>
    public Guid Id { get; }

    /// <summary>
    /// Gets the storage context used by this engine.
    /// </summary>
    public StorageContext StorageContext { get; }

    /// <summary>
    /// Gets the configuration associated with this engine.
    /// </summary>
    public DataVoConfig Config { get; }

    /// <summary>
    /// Gets the engine-local session database bindings.
    /// </summary>
    public SessionDatabaseStore Sessions { get; }

    internal SessionSecurityStore SessionSecurity { get; }

    /// <summary>
    /// Gets the engine catalog facade used by query execution.
    /// </summary>
    public EngineCatalog Catalog { get; }

    /// <summary>
    /// Gets the transaction manager owned by this engine.
    /// </summary>
    public TransactionManager TransactionManager { get; }

    /// <summary>
    /// Gets the table lock manager owned by this engine.
    /// </summary>
    public LockManager LockManager { get; }

    /// <summary>
    /// Gets the primary polymorphic index manager owned by this engine.
    /// </summary>
    public PolyIndexManager IndexManager { get; }

    /// <summary>
    /// Gets the version storage manager owned by this engine for MVCC support.
    /// </summary>
    public VersionStorageManager VersionStorageManager { get; }

    /// <summary>
    /// Gets the transaction ID allocator that assigns globally unique transaction IDs.
    /// </summary>
    public TransactionIdAllocator TransactionIdAllocator { get; }

    /// <summary>
    /// Initializes the active storage runtime and returns an engine wrapper for it.
    /// </summary>
    /// <param name="config">The configuration to initialize.</param>
    /// <returns>A new <see cref="DataVoEngine"/> bound to the initialized storage context.</returns>
    public static DataVoEngine Initialize(DataVoConfig config)
    {
        var storageContext = new StorageContext(config);
        var engine = new DataVoEngine(storageContext);

        SetFallback(engine);

        if (config.StorageMode == StorageMode.Disk && config.WalEnabled)
        {
            new RecoveryManager(config, engine).Recover();
        }

        return engine;
    }

    /// <summary>
    /// Wraps the currently active storage runtime in an engine object.
    /// </summary>
    /// <returns>An engine representing the current process-wide storage context.</returns>
    public static DataVoEngine Current()
    {
        if (ScopedCurrent.Value != null)
        {
            return ScopedCurrent.Value;
        }

        lock (SyncRoot)
        {
            return _fallbackCurrent ??= new DataVoEngine(StorageContext.Instance);
        }
    }

    internal static void ResetCurrent(StorageContext storageContext)
    {
        var next = new DataVoEngine(storageContext);
        DataVoEngine? previous;

        lock (SyncRoot)
        {
            previous = _fallbackCurrent;
            _fallbackCurrent = next;
        }

        if (previous != null && !ReferenceEquals(previous, ScopedCurrent.Value))
        {
            previous.Dispose();
        }
    }

    internal static IDisposable PushCurrent(DataVoEngine engine)
    {
        var previous = ScopedCurrent.Value;
        ScopedCurrent.Value = engine;
        SetFallback(engine);

        return new EngineScope(previous);
    }

    private static void SetFallback(DataVoEngine engine)
    {
        lock (SyncRoot)
        {
            _fallbackCurrent = engine;
        }
    }

    private string ResolveIndexRootDirectory()
    {
        if (Config.StorageMode == StorageMode.Disk)
        {
            return Config.DiskStoragePath ?? "./datavo_data";
        }

        return Path.Combine(Path.GetTempPath(), "datavo_indexes", Id.ToString("N"));
    }

    private sealed class EngineScope(DataVoEngine? previous) : IDisposable
    {
        public void Dispose()
        {
            ScopedCurrent.Value = previous;
        }
    }

    /// <summary>
    /// Authenticates a logical session using configured credential entries.
    /// </summary>
    public bool AuthenticateSession(Guid session, string username, string password)
    {
        return SessionSecurity.Authenticate(session, Config, username, password);
    }

    /// <summary>
    /// Creates a new logical user with hashed credentials and optional initial role membership.
    /// </summary>
    public void CreateUser(string username, string password, string? roleName)
    {
        SessionSecurity.CreateUser(Config, username, password, roleName);
    }

    /// <summary>
    /// Drops an existing logical user.
    /// </summary>
    public void DropUser(string username)
    {
        SessionSecurity.DropUser(Config, username);
    }

    /// <summary>
    /// Creates a custom role that can receive permission grants.
    /// </summary>
    public void CreateRole(string roleName)
    {
        SessionSecurity.CreateRole(roleName);
    }

    /// <summary>
    /// Drops a custom role and removes it from user memberships.
    /// </summary>
    public void DropRole(string roleName)
    {
        SessionSecurity.DropRole(roleName);
    }

    /// <summary>
    /// Grants one or more permissions directly to a user.
    /// </summary>
    public void GrantPermissionsToUser(string username, IEnumerable<DatabasePermission> permissions)
    {
        SessionSecurity.GrantPermissionsToUser(username, permissions);
    }

    /// <summary>
    /// Revokes one or more direct permissions from a user.
    /// </summary>
    public void RevokePermissionsFromUser(string username, IEnumerable<DatabasePermission> permissions)
    {
        SessionSecurity.RevokePermissionsFromUser(username, permissions);
    }

    /// <summary>
    /// Grants a role membership to a user.
    /// </summary>
    public void GrantRoleToUser(string roleName, string username)
    {
        SessionSecurity.GrantRoleToUser(roleName, username);
    }

    /// <summary>
    /// Revokes a role membership from a user.
    /// </summary>
    public void RevokeRoleFromUser(string roleName, string username)
    {
        SessionSecurity.RevokeRoleFromUser(roleName, username);
    }

    /// <summary>
    /// Grants one or more permissions to a role.
    /// </summary>
    public void GrantPermissionsToRole(string roleName, IEnumerable<DatabasePermission> permissions)
    {
        SessionSecurity.GrantPermissionsToRole(roleName, permissions);
    }

    /// <summary>
    /// Revokes one or more permissions from a role.
    /// </summary>
    public void RevokePermissionsFromRole(string roleName, IEnumerable<DatabasePermission> permissions)
    {
        SessionSecurity.RevokePermissionsFromRole(roleName, permissions);
    }

    /// <summary>
    /// Returns a snapshot of known users with role memberships and direct permissions.
    /// </summary>
    internal IReadOnlyList<SecurityUserView> ListUsers()
    {
        return SessionSecurity.ListUsers(Config);
    }

    /// <summary>
    /// Returns a snapshot of known roles and their permissions.
    /// </summary>
    internal IReadOnlyList<SecurityRoleView> ListRoles()
    {
        return SessionSecurity.ListRoles(Config);
    }

    /// <summary>
    /// Returns a flattened snapshot of explicit grants across users and roles.
    /// </summary>
    internal IReadOnlyList<SecurityGrantView> ListGrants()
    {
        return SessionSecurity.ListGrants(Config);
    }

    /// <summary>
    /// Removes any authenticated identity bound to the logical session.
    /// </summary>
    public void LogoutSession(Guid session)
    {
        SessionSecurity.Remove(session);
    }

    /// <summary>
    /// Gets the principal currently bound to the logical session, if any.
    /// </summary>
    public SessionPrincipal? GetSessionPrincipal(Guid session)
    {
        return SessionSecurity.Get(session);
    }

    /// <summary>
    /// Releases engine-owned disposable runtime resources.
    /// </summary>
    public void Dispose()
    {
        if (_transactionIdStateStore != null)
        {
            _transactionIdStateStore.ForcePersistHighWaterMark(TransactionIdAllocator.GetCurrentHighWaterMark());
        }

        IndexManager.Dispose();
        VersionStorageManager.Dispose();
    }
}
