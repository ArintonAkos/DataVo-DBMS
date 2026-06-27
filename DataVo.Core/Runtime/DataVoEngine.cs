using DataVo.Core.BTree;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime.Changes;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.Runtime.Security;
using DataVo.Core.Runtime.Diagnostics;
using DataVo.Core.Transactions;
using DataVo.Core.MVCC;
using DataVo.Core.Utils;
using System.Runtime.ExceptionServices;
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
    private const int DefaultWalAppenderCapacityBytes = 64 * 1024 * 1024;
    private static DataVoEngine? _fallbackCurrent;
    private readonly ReaderWriterLockSlim _snapshotLock = new(LockRecursionPolicy.NoRecursion);
    private readonly TransactionIdStateStore? _transactionIdStateStore;
    private bool _disposed;

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
        Diagnostics = new DataVoDiagnostics();
        Changes = new ChangeCapture();
        Reactive = new ReactiveRegistry(this);

        if (UsesGroupCommitWal(Config))
        {
            WalFrameAppender = new WalAppender(DefaultWalAppenderCapacityBytes);
            WalGroupCommitter = new GroupCommitter(new WalFileStore(Config.ResolveWalFilePath()));
            WalCheckpointer = new WalCheckpointer(
                durableLsnProvider: () => WalGroupCommitter.DurableLsn,
                flushDataToDisk: StorageContext.FlushBackendToDisk,
                stateStore: new CheckpointStateStore(Config),
                walStore: new WalFileStore(Config.ResolveWalFilePath()),
                intervalMs: Config.WalCheckpointIntervalMs);
        }

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
    /// Gets the runtime diagnostics collector owned by this engine.
    /// </summary>
    public DataVoDiagnostics Diagnostics { get; }

    /// <summary>
    /// Gets the change-capture service that emits a <see cref="Changes.ChangeSet"/> per committed transaction.
    /// </summary>
    public ChangeCapture Changes { get; }

    /// <summary>
    /// Gets the reactive subscription registry implementing pull-drain incremental query notifications.
    /// </summary>
    public ReactiveRegistry Reactive { get; }

    internal WalAppender? WalFrameAppender { get; }

    internal GroupCommitter? WalGroupCommitter { get; }

    internal WalCheckpointer? WalCheckpointer { get; }

    internal static bool UsesGroupCommitWal(DataVoConfig config)
    {
        return config.StorageMode == StorageMode.Disk
            && config.WalEnabled
            && config.IoSchedulerMode == IoSchedulerMode.GroupCommit;
    }

    internal void CommitWalEntryThroughGroupCommit(WalEntry entry)
    {
        if (WalFrameAppender == null || WalGroupCommitter == null)
        {
            throw new InvalidOperationException("The WAL group committer is not enabled for this engine.");
        }

        byte[] payload = WalFileStore.SerializeWalEntryPayload(entry);
        WalFrameReservation reservation = WalFrameAppender.Reserve(
            WalFrameOperationType.TxnCommit,
            tableId: 0,
            rowId: 0,
            payloadLength: payload.Length);
        payload.CopyTo(reservation.PayloadSpan);
        WalFrame frame = reservation.Commit();
        WalGroupCommitter.CommitAsync(frame).AsTask().GetAwaiter().GetResult();
    }

    /// <summary>
    /// Durably records a single zero-allocation update as a binary <see cref="WalFrameOperationType.Update"/>
    /// frame: the new-row bytes are written straight into the WAL ring buffer with no JSON, no dictionary,
    /// and no intermediate heap buffer, then flushed through group commit.
    /// </summary>
    internal void CommitBinaryUpdateFrameThroughGroupCommit(string databaseName, string tableName, long oldRowId, ReadOnlySpan<byte> newRowBytes)
    {
        if (WalFrameAppender == null || WalGroupCommitter == null)
        {
            throw new InvalidOperationException("The WAL group committer is not enabled for this engine.");
        }

        int payloadLength = WalUpdateFramePayload.MeasureSize(databaseName, tableName, newRowBytes.Length);
        WalFrameReservation reservation = WalFrameAppender.Reserve(
            WalFrameOperationType.Update,
            tableId: 0,
            rowId: oldRowId,
            payloadLength);
        WalUpdateFramePayload.Write(reservation.PayloadSpan, databaseName, tableName, oldRowId, newRowBytes);
        WalFrame frame = reservation.Commit();
        WalGroupCommitter.CommitAsync(frame).AsTask().GetAwaiter().GetResult();
    }

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

        if (UsesGroupCommitWal(config))
        {
            // Binary WAL recovery: replay the uncheckpointed tail, then checkpoint through the recovered
            // LSN so the replayed effects are durable and the WAL prefix is pruned before serving traffic.
            long recoveredLsn = new BinaryWalRecovery(config, engine).Recover();
            engine.WalCheckpointer!.CheckpointTo(recoveredLsn);
            engine.WalCheckpointer!.Start();
        }
        else if (config.StorageMode == StorageMode.Disk && config.WalEnabled)
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
            if (_fallbackCurrent == null || _fallbackCurrent._disposed)
            {
                _fallbackCurrent = new DataVoEngine(StorageContext.Instance);
            }

            return _fallbackCurrent;
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

        return new EngineScope(previous);
    }

    /// <summary>
    /// Creates a snapshot of in-memory storage, catalog state, and selected database for a session.
    /// </summary>
    /// <param name="session">The logical session identifier.</param>
    /// <returns>A restorable snapshot of the current engine state.</returns>
    public DataVoSnapshot CreateSnapshot(Guid session)
    {
        using SnapshotLockScope _ = EnterRuntimeWriteScope();

        EnsureInMemorySnapshotsSupported();
        EnsureNoActiveTransactionsForSnapshot("create");

        return CaptureSnapshotNoLock(session);
    }

    /// <summary>
    /// Restores a previously captured snapshot into this engine for a session.
    /// </summary>
    /// <param name="session">The logical session identifier.</param>
    /// <param name="snapshot">The snapshot to restore.</param>
    public void RestoreSnapshot(Guid session, DataVoSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        using SnapshotLockScope _ = EnterRuntimeWriteScope();

        EnsureInMemorySnapshotsSupported();

        if (snapshot.StorageMode != Config.StorageMode)
        {
            throw new InvalidOperationException($"Snapshot storage mode '{snapshot.StorageMode}' cannot be restored into '{Config.StorageMode}'.");
        }

        EnsureNoActiveTransactionsForSnapshot("restore");

        DataVoSnapshot previousState = CaptureSnapshotNoLock(session);

        try
        {
            ApplySnapshotNoLock(session, snapshot);
        }
        catch (Exception restoreError)
        {
            try
            {
                ApplySnapshotNoLock(session, previousState);
            }
            catch (Exception rollbackError)
            {
                throw new InvalidOperationException(
                    "Failed to restore the requested DataVo snapshot and rollback the engine to its previous state.",
                    new AggregateException(restoreError, rollbackError));
            }

            ExceptionDispatchInfo.Capture(restoreError).Throw();
        }
    }

    internal SnapshotLockScope EnterRuntimeReadScope()
    {
        _snapshotLock.EnterReadLock();
        return new SnapshotLockScope(_snapshotLock, isWrite: false);
    }

    internal SnapshotLockScope EnterRuntimeWriteScope()
    {
        _snapshotLock.EnterWriteLock();
        return new SnapshotLockScope(_snapshotLock, isWrite: true);
    }

    private void RebuildAllIndexesFromCatalog()
    {
        using IDisposable _ = PushCurrent(this);

        foreach (string databaseName in Catalog.GetDatabases())
        {
            foreach (string tableName in Catalog.GetTables(databaseName))
            {
                RebuildTableIndexes(databaseName, tableName);
            }
        }
    }

    private void RebuildTableIndexes(string databaseName, string tableName)
    {
        List<IndexFile> indexFiles = Catalog.GetTableIndexes(tableName, databaseName);
        if (indexFiles.Count == 0)
        {
            return;
        }

        Dictionary<long, Dictionary<string, object?>> rows = StorageContext.GetTableContents(tableName, databaseName);

        foreach (IndexFile index in indexFiles)
        {
            string indexName = index.IndexFileName;
            if (string.IsNullOrWhiteSpace(indexName))
            {
                continue;
            }

            if (IndexManager.SupportsVectorIndexType(index.IndexKind))
            {
                RebuildVectorIndex(databaseName, tableName, index, rows);
                continue;
            }

            Dictionary<string, List<long>> values = new(StringComparer.Ordinal);
            foreach (KeyValuePair<long, Dictionary<string, object?>> row in rows)
            {
                if (index.AttributeNames.Any(attr => !row.Value.TryGetValue(attr, out object? value) || value == null))
                {
                    continue;
                }

                string key = IndexKeyEncoder.BuildKeyString(row.Value, index.AttributeNames);
                if (!values.TryGetValue(key, out List<long>? rowIds))
                {
                    rowIds = [];
                    values[key] = rowIds;
                }

                rowIds.Add(row.Key);
            }

            IndexManager.RebuildIndex(values, indexName, tableName, databaseName);
        }
    }

    private void RebuildVectorIndex(
        string databaseName,
        string tableName,
        IndexFile index,
        Dictionary<long, Dictionary<string, object?>> rows)
    {
        string indexName = index.IndexFileName;
        if (index.AttributeNames.Count != 1)
        {
            throw new InvalidOperationException($"Vector index '{indexName}' must reference exactly one column.");
        }

        string vectorColumn = index.AttributeNames[0];
        List<(long RowId, float[] Vector)> vectors = [];

        foreach (KeyValuePair<long, Dictionary<string, object?>> row in rows)
        {
            if (!row.Value.TryGetValue(vectorColumn, out object? rawValue) || rawValue == null)
            {
                continue;
            }

            if (!VectorParser.TryCoerceToVector(rawValue, out float[] vector))
            {
                throw new InvalidOperationException($"Cannot rebuild vector index '{indexName}' because row {row.Key} does not contain a valid vector.");
            }

            vectors.Add((row.Key, vector));
        }

        IndexManager.CreateVectorIndex(
            vectors,
            indexName,
            tableName,
            databaseName,
            metric: "cosine",
            indexType: index.IndexKind);
    }

    private DataVoSnapshot CaptureSnapshotNoLock(Guid session)
    {
        return new DataVoSnapshot(
            Config.StorageMode,
            Catalog.ExportState(),
            Sessions.Get(session),
            StorageContext.CreateInMemorySnapshot());
    }

    private void ApplySnapshotNoLock(Guid session, DataVoSnapshot snapshot)
    {
        StorageContext.RestoreInMemorySnapshot(snapshot.StorageSnapshot);
        Catalog.LoadState(snapshot.CatalogState);
        RefreshStorageSchemaScopeNoLock();
        RestoreSessionSelectionNoLock(session, snapshot.SelectedDatabase);
        VersionStorageManager.Clear();
        IndexManager.ClearRuntimeStateAndDeleteAllIndexes();
        RebuildAllIndexesFromCatalog();
    }

    private void RestoreSessionSelectionNoLock(Guid session, string? selectedDatabase)
    {
        if (string.IsNullOrWhiteSpace(selectedDatabase))
        {
            Sessions.Remove(session);
            return;
        }

        Sessions.Set(session, selectedDatabase);
    }

    private void EnsureInMemorySnapshotsSupported()
    {
        if (Config.StorageMode != StorageMode.InMemory)
        {
            throw new NotSupportedException("DataVo snapshots are currently supported only for StorageMode.InMemory.");
        }
    }

    private void EnsureNoActiveTransactionsForSnapshot(string operation)
    {
        if (TransactionManager.HasAnyActiveTransaction())
        {
            throw new InvalidOperationException($"Cannot {operation} a DataVo snapshot while any session has an active transaction.");
        }
    }

    private void RefreshStorageSchemaScopeNoLock()
    {
        StorageContext.AttachRuntimeCatalog(Catalog, Guid.NewGuid());
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
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        if (_transactionIdStateStore != null)
        {
            _transactionIdStateStore.ForcePersistHighWaterMark(TransactionIdAllocator.GetCurrentHighWaterMark());
        }

        if (WalGroupCommitter != null)
        {
            WalGroupCommitter.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }

        if (WalCheckpointer != null)
        {
            // Final checkpoint: the committer has drained, so DurableLsn is stable. Make the data files
            // durable, advance the watermark, and prune the WAL so a clean restart replays nothing.
            WalCheckpointer.Checkpoint();
            WalCheckpointer.Dispose();
        }

        IndexManager.Dispose();
        VersionStorageManager.Dispose();
        StorageContext.Dispose();
        _snapshotLock.Dispose();

        lock (SyncRoot)
        {
            if (ReferenceEquals(_fallbackCurrent, this))
            {
                _fallbackCurrent = null;
            }
        }
    }

}

internal readonly struct SnapshotLockScope(ReaderWriterLockSlim snapshotLock, bool isWrite) : IDisposable
{
    public void Dispose()
    {
        if (isWrite)
        {
            snapshotLock.ExitWriteLock();
            return;
        }

        snapshotLock.ExitReadLock();
    }
}
