using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.BTree;
using DataVo.Core.Transactions;

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

    private DataVoEngine(StorageContext storageContext)
    {
        Id = Guid.NewGuid();
        StorageContext = storageContext;
        Config = storageContext.Config;
        Sessions = new SessionDatabaseStore();
        Catalog = new EngineCatalog(Config);
        TransactionManager = new TransactionManager();
        LockManager = new LockManager();
        IndexManager = new IndexManager(Config, ResolveIndexRootDirectory());
    }

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
    /// Gets the index manager owned by this engine.
    /// </summary>
    public IndexManager IndexManager { get; }

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
        SetFallback(new DataVoEngine(storageContext));
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
    /// Releases engine-owned disposable runtime resources.
    /// </summary>
    public void Dispose()
    {
        IndexManager.Dispose();
    }
}
