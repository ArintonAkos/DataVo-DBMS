using DataVo.Core.StorageEngine;
using System.Collections.Concurrent;
using System.Threading;
using DataVo.Core.Exceptions;

namespace DataVo.Core.Transactions;

/// <summary>
/// Thread-safe manager of table, row and key locks for one engine.
/// <para>
/// Table locks are keyed by the fully-qualified table identity <c>{database}.{table}</c>, allowing concurrent
/// readers while guaranteeing exclusive access for writers. A row write lock or a key lock also holds the table's
/// lock in shared mode as an <em>intent</em> lock, so exclusive table writers (DDL, bulk writes) wait for every
/// row-level writer and the other way round, while row-level writers of different rows run side by side.
/// </para>
/// <para>
/// A table's lock entry lives as long as the table: releasing the last lock keeps it, so a busy table never
/// allocates or disposes its lock object; <see cref="ForgetTable"/> drops it when the table is dropped. Row and key
/// lock entries come and go with their locks and are recycled through a small pool.
/// </para>
/// </summary>
public sealed class LockManager
{
    private const int DefaultLockAcquireTimeoutMs = 30000;
    private const int MaxPooledRowLocks = 1024;

    private sealed class TableLockEntry(string key)
    {
        /// <summary>The table's lock key, <c>{database}.{table}</c>.</summary>
        public string Key { get; } = key;

        public ReaderWriterLockSlim Lock { get; } = new(LockRecursionPolicy.NoRecursion);

        /// <summary>Threads that retained the entry and have not released it yet; it is only forgotten at zero.</summary>
        public int ActiveUsers;

        /// <summary>1 once <see cref="ForgetTable"/> started dropping the entry; a retainer that sees it backs off.</summary>
        public int Forgotten;

        public readonly object OwnershipSync = new();
        public readonly Dictionary<int, int> ReadOwnerCounts = [];
        public int? WriteOwnerThreadId;
    }

    private sealed class RowLockEntry
    {
        public LockId Id;
        public ReaderWriterLockSlim Lock { get; } = new(LockRecursionPolicy.NoRecursion);
        public int ActiveUsers;
        public readonly object OwnershipSync = new();
        public readonly HashSet<int> ReadOwnerThreadIds = [];
        public int? WriteOwnerThreadId;

        /// <summary>The table whose intent lock this row write holds, or null when the thread held the table write lock.</summary>
        public TableLockEntry? TableIntent;

        /// <summary>Clears the entry for reuse; it is idle (no owner, no waiter) when this is called.</summary>
        public void Reset()
        {
            Id = default;
            ActiveUsers = 0;
            WriteOwnerThreadId = null;
            TableIntent = null;
            ReadOwnerThreadIds.Clear();
        }
    }

    /// <summary>
    /// Identifies a row lock (<see cref="Scope"/> null) or a key lock: a value of a primary or unique key that an
    /// insert reserves, whether or not a row holds it yet. Compared without allocating.
    /// </summary>
    private readonly struct LockId(string database, string table, string? scope, RowKey row, string? text)
    {
        public string Database { get; } = database;
        public string Table { get; } = table;

        /// <summary>The key constraint for a key lock (the primary key or a unique column); null for a row lock.</summary>
        public string? Scope { get; } = scope;

        /// <summary>The row, or the value of an integer key.</summary>
        public RowKey Row { get; } = row;

        /// <summary>The encoded value of a non-integer key; null otherwise.</summary>
        public string? Text { get; } = text;

        public override string ToString() => Scope is null
            ? $"{Database}.{Table}#row:{Row}"
            : $"{Database}.{Table}#key:{Scope}:{Text ?? Row.ToString()}";
    }

    private sealed class LockIdComparer : IEqualityComparer<LockId>
    {
        public static readonly LockIdComparer Instance = new();

        public bool Equals(LockId x, LockId y) =>
            x.Row == y.Row
            && string.Equals(x.Text, y.Text, StringComparison.Ordinal)
            && string.Equals(x.Table, y.Table, StringComparison.OrdinalIgnoreCase)
            && string.Equals(x.Database, y.Database, StringComparison.OrdinalIgnoreCase)
            && string.Equals(x.Scope, y.Scope, StringComparison.OrdinalIgnoreCase);

        public int GetHashCode(LockId id) => HashCode.Combine(
            id.Row,
            StringComparer.OrdinalIgnoreCase.GetHashCode(id.Database),
            StringComparer.OrdinalIgnoreCase.GetHashCode(id.Table),
            id.Scope is null ? 0 : StringComparer.OrdinalIgnoreCase.GetHashCode(id.Scope),
            id.Text is null ? 0 : StringComparer.Ordinal.GetHashCode(id.Text));
    }

    private readonly record struct TableName(string Database, string Table);

    private sealed class TableNameComparer : IEqualityComparer<TableName>
    {
        public static readonly TableNameComparer Instance = new();

        public bool Equals(TableName x, TableName y) =>
            string.Equals(x.Table, y.Table, StringComparison.OrdinalIgnoreCase)
            && string.Equals(x.Database, y.Database, StringComparison.OrdinalIgnoreCase);

        public int GetHashCode(TableName name) => HashCode.Combine(
            StringComparer.OrdinalIgnoreCase.GetHashCode(name.Database),
            StringComparer.OrdinalIgnoreCase.GetHashCode(name.Table));
    }

    private readonly record struct WaitRegistration(int ThreadId, bool Active)
    {
        public static readonly WaitRegistration None = new(0, false);
    }

    // Locks the current thread holds, across all lock managers: a writer that already holds locks must not queue
    // behind waiting snapshot readers (see SnapshotPublicationGate). Thread-static like the locks themselves (a
    // ReaderWriterLockSlim is thread-affine): statements acquire and release on one thread and never await while
    // holding a lock; asynchronous work in the engine is blocked on synchronously by the calling thread.
    [ThreadStatic] private static int t_heldLocks;

    /// <summary>Gets whether the current thread holds any table or row lock.</summary>
    internal static bool CurrentThreadHoldsLocks => t_heldLocks > 0;

    // Table entries by lock key, and by (database, table) so the row-level paths find them without building the key.
    // Both are read without a lock; entries are only added and forgotten under _tableLockLifecycleSync.
    private readonly ConcurrentDictionary<string, TableLockEntry> _tableLocks =
        new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<TableName, TableLockEntry> _tableLocksByName =
        new(TableNameComparer.Instance);

    // Row and key lock entries; read and written only under _rowLockLifecycleSync.
    private readonly Dictionary<LockId, RowLockEntry> _rowLocks = new(LockIdComparer.Instance);
    private readonly Stack<RowLockEntry> _rowLockPool = new();
    private readonly object _tableLockLifecycleSync = new();
    private readonly object _rowLockLifecycleSync = new();
    private readonly object _waitForGraphSync = new();
    private readonly int _lockAcquireTimeoutMs;
    private readonly Dictionary<int, HashSet<int>> _waitForGraph = [];
    private readonly Dictionary<int, string> _waitingScopes = [];
    private readonly Dictionary<int, (object Entry, bool Write)> _waitTargets = [];

    /// <summary>Called after a thread is recorded as waiting and before it blocks. Lets tests hold it at that point.</summary>
    internal Action? WaitRegisteredForTesting { get; set; }
    private int _activeRowWriteLocks;
    private long _rowWriteLockEpoch;

    /// <summary>
    /// Gets a value indicating whether any row-level write lock is currently held or being acquired. Key locks taken
    /// by inserts do not count: an insert adds a row and never changes one a reader may be reading.
    /// </summary>
    internal bool HasActiveRowWriteLocks => Volatile.Read(ref _activeRowWriteLocks) > 0;

    /// <summary>
    /// Gets a counter that advances whenever a row write lock is requested. A multi-row reader that saw no
    /// active write locks can compare this before and after its read to detect a writer that started meanwhile.
    /// </summary>
    internal long RowWriteLockEpoch => Volatile.Read(ref _rowWriteLockEpoch);

    /// <summary>
    /// Creates a lock manager instance.
    /// </summary>
    /// <remarks>Each engine owns one instance.</remarks>
    public LockManager(int lockAcquireTimeoutMs = DefaultLockAcquireTimeoutMs)
    {
        if (lockAcquireTimeoutMs < -1)
        {
            throw new ArgumentOutOfRangeException(nameof(lockAcquireTimeoutMs), "Timeout must be -1 (infinite) or a non-negative value.");
        }

        _lockAcquireTimeoutMs = lockAcquireTimeoutMs;
    }

    /// <summary>
    /// Acquires a table-level read lock for a database/table pair.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    public void AcquireReadLock(string databaseName, string tableName)
    {
        AcquireTableLock(RetainTableLock(databaseName, tableName), write: false);
    }

    /// <summary>
    /// Acquires a table-level write lock for a database/table pair.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    public void AcquireWriteLock(string databaseName, string tableName)
    {
        AcquireTableLock(RetainTableLock(databaseName, tableName), write: true);
    }

    /// <summary>
    /// Releases a table-level read lock for a database/table pair.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    public void ReleaseReadLock(string databaseName, string tableName)
    {
        ReleaseTableLock(FindTableLock(databaseName, tableName, "read"), write: false);
    }

    /// <summary>
    /// Releases a table-level write lock for a database/table pair.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    public void ReleaseWriteLock(string databaseName, string tableName)
    {
        ReleaseTableLock(FindTableLock(databaseName, tableName, "write"), write: true);
    }

    /// <summary>
    /// Acquires a row-level read lock.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="rowId">The row identifier.</param>
    public void AcquireRowReadLock(string databaseName, string tableName, RowKey rowId)
    {
        var id = new LockId(databaseName, tableName, scope: null, rowId, text: null);
        if (Runtime.DeferredPublication.Current is { } publication && publication.TryReacquireHeldLock("R:" + id))
        {
            return;
        }

        RowLockEntry rowLock = RetainRowLock(id);
        int threadId = Environment.CurrentManagedThreadId;
        WaitRegistration waitRegistration = WaitRegistration.None;

        try
        {
            waitRegistration = RegisterWaitIfBlocked(rowLock, "row read", GetRowBlockingThreads(rowLock, threadId, write: false), threadId, write: false);

            if (!rowLock.Lock.TryEnterReadLock(_lockAcquireTimeoutMs))
            {
                throw new TimeoutException($"Timed out acquiring row read lock for '{id}'.");
            }

            // See AcquireTableLock: edges must be gone before ownership becomes visible.
            waitRegistration = ClearWaitRegistrationEarly(waitRegistration);
            RegisterRowReadOwner(rowLock, threadId);
        }
        catch
        {
            ReleaseRowLock(rowLock);
            throw;
        }
        finally
        {
            ClearWaitRegistration(waitRegistration);
        }
    }

    /// <summary>
    /// Acquires a row-level write lock, coordinated with a table-level intent read lock so exclusive table writers
    /// (such as <see cref="AcquireWriteLock(string, string)"/>) never run concurrently with row-level writers.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="rowId">The row identifier.</param>
    public void AcquireRowWriteLock(string databaseName, string tableName, RowKey rowId)
    {
        AcquireRowEntryWriteLock(new LockId(databaseName, tableName, scope: null, rowId, text: null), "row write", countsAsRowWrite: true);
    }

    /// <summary>
    /// Releases a row-level read lock.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="rowId">The row identifier.</param>
    public void ReleaseRowReadLock(string databaseName, string tableName, RowKey rowId)
    {
        var id = new LockId(databaseName, tableName, scope: null, rowId, text: null);
        if (Runtime.DeferredPublication.Current is { } publication
            && publication.TryHandleRelease("R:" + id, write: false, static () => { }))
        {
            return;
        }

        RowLockEntry rowLock = FindRowLock(id, "read");
        UnregisterRowReadOwner(rowLock, Environment.CurrentManagedThreadId);
        rowLock.Lock.ExitReadLock();
        ReleaseRowLock(rowLock);
    }

    /// <summary>
    /// Releases a row-level write lock.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="rowId">The row identifier.</param>
    public void ReleaseRowWriteLock(string databaseName, string tableName, RowKey rowId)
    {
        ReleaseRowEntryWriteLock(new LockId(databaseName, tableName, scope: null, rowId, text: null), countsAsRowWrite: true);
    }

    /// <summary>
    /// Reserves a value of an integer primary or unique key for an insert or a key-changing update: holds the table's
    /// intent lock plus an exclusive lock on the value, so two writers giving rows the same value check for it one
    /// after the other, and the second sees the first's row. Release with <see cref="ReleaseKeyLock(string, string, string, long)"/>;
    /// inside a durable write batch the release waits for publication, like a row write lock.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="constraint">The constraint the value belongs to: the primary-key index name, or a unique column.</param>
    /// <param name="key">The key value.</param>
    /// <returns>
    /// <see langword="false"/> when nothing was locked because the current thread holds the table's write lock, which
    /// already excludes every other writer; then nothing must be released.
    /// </returns>
    internal bool AcquireKeyLock(string databaseName, string tableName, string constraint, long key) =>
        AcquireKeyLock(new LockId(databaseName, tableName, constraint, RowKey.FromInt64(key), text: null));

    /// <summary>
    /// Reserves an encoded key value; see <see cref="AcquireKeyLock(string, string, string, long)"/>.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="constraint">The constraint the value belongs to: the primary-key index name, or a unique column.</param>
    /// <param name="encodedKey">The key value, encoded as the key's index stores it.</param>
    /// <returns><see langword="false"/> when the current thread holds the table's write lock and nothing was locked.</returns>
    internal bool AcquireKeyLock(string databaseName, string tableName, string constraint, string encodedKey) =>
        AcquireKeyLock(new LockId(databaseName, tableName, constraint, default, encodedKey));

    /// <summary>Releases a key lock taken by <see cref="AcquireKeyLock(string, string, string, long)"/>.</summary>
    internal void ReleaseKeyLock(string databaseName, string tableName, string constraint, long key) =>
        ReleaseRowEntryWriteLock(new LockId(databaseName, tableName, constraint, RowKey.FromInt64(key), text: null), countsAsRowWrite: false);

    /// <summary>Releases a key lock taken by <see cref="AcquireKeyLock(string, string, string, string)"/>.</summary>
    internal void ReleaseKeyLock(string databaseName, string tableName, string constraint, string encodedKey) =>
        ReleaseRowEntryWriteLock(new LockId(databaseName, tableName, constraint, default, encodedKey), countsAsRowWrite: false);

    /// <summary>
    /// Acquires the table's intent lock for a writer that then locks rows or keys of its own: it shares the table
    /// with other such writers and excludes exclusive table writers. Pair with <see cref="ReleaseTableIntentLock"/>,
    /// passing the result.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <returns><see langword="false"/> when the current thread already holds the table's write lock and nothing was taken.</returns>
    internal bool AcquireTableIntentLock(string databaseName, string tableName)
    {
        return AcquireTableIntent(RetainTableLock(databaseName, tableName));
    }

    /// <summary>
    /// Releases an intent lock taken by <see cref="AcquireTableIntentLock"/>. Inside a durable write batch the release
    /// waits for publication, so no exclusive table writer sees the statement half published.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="acquired">What <see cref="AcquireTableIntentLock"/> returned.</param>
    internal void ReleaseTableIntentLock(string databaseName, string tableName, bool acquired)
    {
        if (!acquired)
        {
            return;
        }

        TableLockEntry tableLock = FindTableLock(databaseName, tableName, "intent");
        if (Runtime.DeferredPublication.Current is { } publication)
        {
            publication.DeferRelease(() => ReleaseTableRead(tableLock));
            return;
        }

        ReleaseTableRead(tableLock);
    }

    /// <summary>
    /// Drops the lock entry of a table that no longer exists, unless a thread is using it. Called after a table is
    /// dropped; a table that is created again later gets a new entry on its first lock.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    internal void ForgetTable(string databaseName, string tableName)
    {
        lock (_tableLockLifecycleSync)
        {
            if (_tableLocks.TryGetValue(BuildTableKey(databaseName, tableName), out TableLockEntry? entry))
            {
                ForgetTableEntryNoLock(entry);
            }
        }
    }

    /// <summary>Drops the lock entries of a dropped database's tables, except those a thread is using.</summary>
    /// <param name="databaseName">The database name.</param>
    internal void ForgetDatabase(string databaseName)
    {
        string prefix = databaseName + ".";
        lock (_tableLockLifecycleSync)
        {
            foreach (KeyValuePair<string, TableLockEntry> pair in _tableLocks)
            {
                if (pair.Key.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                {
                    ForgetTableEntryNoLock(pair.Value);
                }
            }
        }
    }

    /// <summary>
    /// Acquires row-level write locks in deterministic order.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="rowIds">Row identifiers to lock.</param>
    /// <returns>The ordered lock acquisition list.</returns>
    public List<RowKey> AcquireRowWriteLocks(string databaseName, string tableName, IEnumerable<RowKey> rowIds)
    {
        List<RowKey> ordered = BuildOrderedRowLockList(rowIds);

        for (int i = 0; i < ordered.Count; i++)
        {
            AcquireRowWriteLock(databaseName, tableName, ordered[i]);
        }

        return ordered;
    }

    /// <summary>
    /// Releases row-level write locks in reverse order.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="rowIds">Row identifiers to unlock.</param>
    public void ReleaseRowWriteLocks(string databaseName, string tableName, IReadOnlyList<RowKey> rowIds)
    {
        for (int i = rowIds.Count - 1; i >= 0; i--)
        {
            ReleaseRowWriteLock(databaseName, tableName, rowIds[i]);
        }
    }

    /// <summary>
    /// Releases row-level write locks in reverse order without deferring them into an active write batch.
    /// Intended for locks released before any data mutation occurs (for example, when retrying after a
    /// concurrent out-of-place update relocates a target row).
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="rowIds">Row identifiers to unlock.</param>
    /// <example>
    /// <code>
    /// locks.ReleaseRowWriteLocksImmediately("AppDb", "Users", staleRowLocks);
    /// </code>
    /// </example>
    internal void ReleaseRowWriteLocksImmediately(string databaseName, string tableName, IReadOnlyList<RowKey> rowIds)
    {
        for (int i = rowIds.Count - 1; i >= 0; i--)
        {
            var id = new LockId(databaseName, tableName, scope: null, rowIds[i], text: null);
            if (Runtime.DeferredPublication.Current is { } publication
                && publication.TryReleaseHeldWriteLockIfDeferred("R:" + id))
            {
                continue;
            }

            ReleaseRowEntryWriteLockCore(id, countsAsRowWrite: true);
        }
    }

    /// <summary>
    /// Acquires row-level read locks in deterministic order.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="rowIds">Row identifiers to lock.</param>
    /// <returns>The ordered lock acquisition list.</returns>
    public List<RowKey> AcquireRowReadLocks(string databaseName, string tableName, IEnumerable<RowKey> rowIds)
    {
        List<RowKey> ordered = BuildOrderedRowLockList(rowIds);

        for (int i = 0; i < ordered.Count; i++)
        {
            AcquireRowReadLock(databaseName, tableName, ordered[i]);
        }

        return ordered;
    }

    /// <summary>
    /// Releases row-level read locks in reverse order.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="rowIds">Row identifiers to unlock.</param>
    public void ReleaseRowReadLocks(string databaseName, string tableName, IReadOnlyList<RowKey> rowIds)
    {
        for (int i = rowIds.Count - 1; i >= 0; i--)
        {
            ReleaseRowReadLock(databaseName, tableName, rowIds[i]);
        }
    }

    /// <summary>
    /// Acquires a table-scoped read lock using a pre-composed lock key in the form <c>{database}.{table}</c>.
    /// </summary>
    /// <remarks>
    /// Use this overload when the caller already normalized the table identity and wants to avoid key recomposition.
    /// </remarks>
    public void AcquireReadLock(string tableKey)
    {
        AcquireTableLock(RetainTableLock(tableKey), write: false);
    }

    /// <summary>
    /// Acquires a table-scoped write lock using a pre-composed lock key in the form <c>{database}.{table}</c>.
    /// </summary>
    public void AcquireWriteLock(string tableKey)
    {
        AcquireTableLock(RetainTableLock(tableKey), write: true);
    }

    /// <summary>
    /// Acquires a table-scoped write lock. Kept for callers that hold the key; every table entry is retained now.
    /// </summary>
    internal void AcquireRetainedWriteLock(string tableKey)
    {
        AcquireWriteLock(tableKey);
    }

    /// <summary>
    /// Releases a table-scoped read lock acquired through the table-key overloads.
    /// </summary>
    public void ReleaseReadLock(string tableKey)
    {
        ReleaseTableLock(FindTableLock(tableKey, "read"), write: false);
    }

    /// <summary>
    /// Releases a table-scoped write lock acquired through the table-key overloads.
    /// </summary>
    public void ReleaseWriteLock(string tableKey)
    {
        ReleaseTableLock(FindTableLock(tableKey, "write"), write: true);
    }

    /// <summary>
    /// Releases a table-scoped write lock taken by <see cref="AcquireRetainedWriteLock"/>.
    /// </summary>
    internal void ReleaseRetainedWriteLock(string tableKey)
    {
        ReleaseWriteLock(tableKey);
    }

    /// <summary>
    /// Takes the table's lock in shared mode as the intent lock of a row-level writer. Skips acquisition when the
    /// current thread already holds the table write lock, and counts a repeat by a thread that already holds it
    /// shared. Consumes the caller's retention of <paramref name="tableLock"/>.
    /// </summary>
    /// <returns><see langword="false"/> when nothing was taken because the thread holds the table write lock.</returns>
    private bool AcquireTableIntent(TableLockEntry tableLock)
    {
        int threadId = Environment.CurrentManagedThreadId;

        bool alreadyWriteOwner = false;
        bool alreadyReadOwner = false;
        lock (tableLock.OwnershipSync)
        {
            if (tableLock.WriteOwnerThreadId == threadId)
            {
                alreadyWriteOwner = true;
            }
            else if (tableLock.ReadOwnerCounts.TryGetValue(threadId, out int count) && count > 0)
            {
                tableLock.ReadOwnerCounts[threadId] = count + 1;
                alreadyReadOwner = true;
            }
        }

        if (alreadyWriteOwner)
        {
            ReleaseTableLockEntry(tableLock);
            return false;
        }

        if (alreadyReadOwner)
        {
            return true;
        }

        WaitRegistration waitRegistration = WaitRegistration.None;
        try
        {
            waitRegistration = RegisterWaitIfBlocked(tableLock, "table read", GetTableBlockingThreads(tableLock, threadId, write: false), threadId, write: false);

            if (!tableLock.Lock.TryEnterReadLock(_lockAcquireTimeoutMs))
            {
                throw new TimeoutException($"Timed out acquiring table read lock for '{tableLock.Key}'.");
            }

            waitRegistration = ClearWaitRegistrationEarly(waitRegistration);
            RegisterTableReadOwner(tableLock, threadId);
            return true;
        }
        catch
        {
            ReleaseTableLockEntry(tableLock);
            throw;
        }
        finally
        {
            ClearWaitRegistration(waitRegistration);
        }
    }

    /// <summary>Takes a row or key lock exclusively, with the table's intent lock first.</summary>
    private void AcquireRowEntryWriteLock(LockId id, string lockScope, bool countsAsRowWrite)
    {
        if (Runtime.DeferredPublication.Current is { } publication && publication.TryReacquireHeldLock("R:" + id))
        {
            return;
        }

        TableLockEntry tableLock = RetainTableLock(id.Database, id.Table);
        bool acquiredTableIntent = AcquireTableIntent(tableLock);

        RowLockEntry rowLock = RetainRowLock(id);
        if (countsAsRowWrite)
        {
            Interlocked.Increment(ref _activeRowWriteLocks);
            Interlocked.Increment(ref _rowWriteLockEpoch);
        }

        int threadId = Environment.CurrentManagedThreadId;
        WaitRegistration waitRegistration = WaitRegistration.None;

        try
        {
            waitRegistration = RegisterWaitIfBlocked(rowLock, lockScope, GetRowBlockingThreads(rowLock, threadId, write: true), threadId, write: true);

            if (!rowLock.Lock.TryEnterWriteLock(_lockAcquireTimeoutMs))
            {
                throw new TimeoutException($"Timed out acquiring {lockScope} lock for '{id}'.");
            }

            // See AcquireTableLock: edges must be gone before ownership becomes visible.
            waitRegistration = ClearWaitRegistrationEarly(waitRegistration);
            RegisterRowWriteOwner(rowLock, threadId);
            rowLock.TableIntent = acquiredTableIntent ? tableLock : null;
        }
        catch
        {
            if (countsAsRowWrite)
            {
                Interlocked.Decrement(ref _activeRowWriteLocks);
            }

            ReleaseRowLock(rowLock);
            if (acquiredTableIntent)
            {
                ReleaseTableRead(tableLock);
            }

            throw;
        }
        finally
        {
            ClearWaitRegistration(waitRegistration);
        }
    }

    private bool AcquireKeyLock(LockId id)
    {
        if (Runtime.DeferredPublication.Current is { } publication && publication.TryReacquireHeldLock("R:" + id))
        {
            return true;
        }

        // The table write lock already keeps every other writer out; a key lock under it would only cost.
        TableLockEntry tableLock = RetainTableLock(id.Database, id.Table);
        bool tableWriteOwner;
        lock (tableLock.OwnershipSync)
        {
            tableWriteOwner = tableLock.WriteOwnerThreadId == Environment.CurrentManagedThreadId;
        }

        ReleaseTableLockEntry(tableLock);
        if (tableWriteOwner)
        {
            return false;
        }

        AcquireRowEntryWriteLock(id, "key write", countsAsRowWrite: false);
        return true;
    }

    private void ReleaseRowEntryWriteLock(LockId id, bool countsAsRowWrite)
    {
        if (Runtime.DeferredPublication.Current is { } publication
            && publication.TryHandleRelease("R:" + id, write: true, () => ReleaseRowEntryWriteLockCore(id, countsAsRowWrite)))
        {
            return;
        }

        ReleaseRowEntryWriteLockCore(id, countsAsRowWrite);
    }

    private void ReleaseRowEntryWriteLockCore(LockId id, bool countsAsRowWrite)
    {
        RowLockEntry rowLock = FindRowLock(id, "write");
        TableLockEntry? tableIntent = rowLock.TableIntent;
        rowLock.TableIntent = null;

        UnregisterRowWriteOwner(rowLock, Environment.CurrentManagedThreadId);
        rowLock.Lock.ExitWriteLock();
        ReleaseRowLock(rowLock);
        if (countsAsRowWrite)
        {
            Interlocked.Decrement(ref _activeRowWriteLocks);
        }

        if (tableIntent is not null)
        {
            ReleaseTableRead(tableIntent);
        }
    }

    /// <summary>Takes a table lock. Consumes the caller's retention of <paramref name="tableLock"/>.</summary>
    private void AcquireTableLock(TableLockEntry tableLock, bool write)
    {
        // A write lock whose release is deferred until publication is still held by this flow.
        if (Runtime.DeferredPublication.Current is { } publication && publication.TryReacquireHeldLock("T:" + tableLock.Key))
        {
            ReleaseTableLockEntry(tableLock);
            return;
        }

        int threadId = Environment.CurrentManagedThreadId;

        if (!write)
        {
            lock (tableLock.OwnershipSync)
            {
                if (tableLock.ReadOwnerCounts.TryGetValue(threadId, out int count) && count > 0)
                {
                    tableLock.ReadOwnerCounts[threadId] = count + 1;
                    return;
                }
            }
        }

        WaitRegistration waitRegistration = WaitRegistration.None;

        try
        {
            waitRegistration = RegisterWaitIfBlocked(tableLock, write ? "table write" : "table read", GetTableBlockingThreads(tableLock, threadId, write), threadId, write);

            if (write)
            {
                if (!tableLock.Lock.TryEnterWriteLock(_lockAcquireTimeoutMs))
                {
                    throw new TimeoutException($"Timed out acquiring table write lock for '{tableLock.Key}'.");
                }

                // Clear wait edges BEFORE becoming a visible owner: a thread that is owner while its
                // stale edge (to the previous owner) is still in the graph lets that previous owner,
                // re-requesting the same lock, walk the stale edge into a phantom cycle.
                waitRegistration = ClearWaitRegistrationEarly(waitRegistration);
                RegisterTableWriteOwner(tableLock, threadId);
            }
            else
            {
                if (!tableLock.Lock.TryEnterReadLock(_lockAcquireTimeoutMs))
                {
                    throw new TimeoutException($"Timed out acquiring table read lock for '{tableLock.Key}'.");
                }

                waitRegistration = ClearWaitRegistrationEarly(waitRegistration);
                RegisterTableReadOwner(tableLock, threadId);
            }
        }
        catch
        {
            ReleaseTableLockEntry(tableLock);
            throw;
        }
        finally
        {
            ClearWaitRegistration(waitRegistration);
        }
    }

    private void ReleaseTableLock(TableLockEntry tableLock, bool write)
    {
        // Inside a durable write batch, write locks stay held until the statement is published.
        if (Runtime.DeferredPublication.Current is { } publication
            && publication.TryHandleRelease("T:" + tableLock.Key, write, () => ReleaseTableLockCore(tableLock, write)))
        {
            return;
        }

        ReleaseTableLockCore(tableLock, write);
    }

    private void ReleaseTableLockCore(TableLockEntry tableLock, bool write)
    {
        if (!write)
        {
            ReleaseTableRead(tableLock);
            return;
        }

        UnregisterTableWriteOwner(tableLock, Environment.CurrentManagedThreadId);
        tableLock.Lock.ExitWriteLock();
        ReleaseTableLockEntry(tableLock);
    }

    private void ReleaseTableRead(TableLockEntry tableLock)
    {
        bool exitReadLock = UnregisterTableReadOwner(tableLock, Environment.CurrentManagedThreadId);
        if (exitReadLock)
        {
            tableLock.Lock.ExitReadLock();
        }

        ReleaseTableLockEntry(tableLock);
    }

    private TableLockEntry RetainTableLock(string databaseName, string tableName)
    {
        var name = new TableName(databaseName, tableName);
        if (_tableLocksByName.TryGetValue(name, out TableLockEntry? existing) && TryRetain(existing))
        {
            return existing;
        }

        lock (_tableLockLifecycleSync)
        {
            if (!_tableLocksByName.TryGetValue(name, out TableLockEntry? entry) || Volatile.Read(ref entry.Forgotten) != 0)
            {
                entry = _tableLocks.GetOrAdd(BuildTableKey(databaseName, tableName), static key => new TableLockEntry(key));
                _tableLocksByName[name] = entry;
            }

            Interlocked.Increment(ref entry.ActiveUsers);
            return entry;
        }
    }

    private TableLockEntry RetainTableLock(string tableKey)
    {
        if (_tableLocks.TryGetValue(tableKey, out TableLockEntry? existing) && TryRetain(existing))
        {
            return existing;
        }

        lock (_tableLockLifecycleSync)
        {
            TableLockEntry entry = _tableLocks.GetOrAdd(tableKey, static key => new TableLockEntry(key));
            Interlocked.Increment(ref entry.ActiveUsers);
            return entry;
        }
    }

    /// <summary>
    /// Retains an entry found without the lifecycle lock. Fails when <see cref="ForgetTable"/> is dropping it: the
    /// increment and the check are both full fences, and so are the forgetter's mark and count check, so either the
    /// forgetter sees this user and keeps the entry, or this user sees the mark and takes the locked path.
    /// </summary>
    private static bool TryRetain(TableLockEntry entry)
    {
        Interlocked.Increment(ref entry.ActiveUsers);
        if (Volatile.Read(ref entry.Forgotten) == 0)
        {
            return true;
        }

        Interlocked.Decrement(ref entry.ActiveUsers);
        return false;
    }

    private static void ReleaseTableLockEntry(TableLockEntry tableLock)
    {
        // The entry stays for the next user; only ForgetTable drops it.
        Interlocked.Decrement(ref tableLock.ActiveUsers);
    }

    private void ForgetTableEntryNoLock(TableLockEntry entry)
    {
        Interlocked.Exchange(ref entry.Forgotten, 1);
        if (Volatile.Read(ref entry.ActiveUsers) != 0)
        {
            Interlocked.Exchange(ref entry.Forgotten, 0);
            return;
        }

        _tableLocks.TryRemove(entry.Key, out _);
        foreach (KeyValuePair<TableName, TableLockEntry> pair in _tableLocksByName)
        {
            if (ReferenceEquals(pair.Value, entry))
            {
                _tableLocksByName.TryRemove(pair.Key, out _);
            }
        }

        entry.Lock.Dispose();
    }

    private TableLockEntry FindTableLock(string databaseName, string tableName, string mode)
    {
        if (_tableLocksByName.TryGetValue(new TableName(databaseName, tableName), out TableLockEntry? entry)
            || _tableLocks.TryGetValue(BuildTableKey(databaseName, tableName), out entry))
        {
            return entry;
        }

        throw new InvalidOperationException($"Table {mode} lock not found for key '{BuildTableKey(databaseName, tableName)}'.");
    }

    private TableLockEntry FindTableLock(string tableKey, string mode)
    {
        return _tableLocks.TryGetValue(tableKey, out TableLockEntry? entry)
            ? entry
            : throw new InvalidOperationException($"Table {mode} lock not found for key '{tableKey}'.");
    }

    private RowLockEntry RetainRowLock(LockId id)
    {
        lock (_rowLockLifecycleSync)
        {
            if (!_rowLocks.TryGetValue(id, out RowLockEntry? rowLock))
            {
                rowLock = _rowLockPool.Count > 0 ? _rowLockPool.Pop() : new RowLockEntry();
                rowLock.Id = id;
                _rowLocks.Add(id, rowLock);
            }

            rowLock.ActiveUsers++;
            return rowLock;
        }
    }

    private RowLockEntry FindRowLock(LockId id, string mode)
    {
        lock (_rowLockLifecycleSync)
        {
            if (_rowLocks.TryGetValue(id, out RowLockEntry? rowLock))
            {
                return rowLock;
            }
        }

        string scope = id.Scope is null ? "Row" : "Key";
        throw new InvalidOperationException($"{scope} {mode} lock not found for key '{id}'.");
    }

    private void ReleaseRowLock(RowLockEntry rowLock)
    {
        lock (_rowLockLifecycleSync)
        {
            if (!_rowLocks.TryGetValue(rowLock.Id, out RowLockEntry? current)
                || !ReferenceEquals(current, rowLock))
            {
                return;
            }

            current.ActiveUsers--;
            if (current.ActiveUsers > 0)
            {
                return;
            }

            // Idle: nobody owns it and nobody waits (a waiter counts as a user), so it can be reused as is.
            _rowLocks.Remove(current.Id);
            current.Reset();
            if (_rowLockPool.Count < MaxPooledRowLocks)
            {
                _rowLockPool.Push(current);
            }
            else
            {
                current.Lock.Dispose();
            }
        }
    }

    private static List<RowKey> BuildOrderedRowLockList(IEnumerable<RowKey> rowIds)
    {
        return rowIds
            .Where(id => id != default) // default marks "no row"; clustered keys may be zero or negative
            .Distinct()
            .OrderBy(id => id)
            .ToList();
    }

    private static string BuildTableKey(string databaseName, string tableName)
    {
        return $"{databaseName}.{tableName}";
    }

    /// <summary>The threads a table lock request would wait for; null when there are none (the common case, no allocation).</summary>
    private static HashSet<int>? GetTableBlockingThreads(TableLockEntry tableLock, int threadId, bool write)
    {
        lock (tableLock.OwnershipSync)
        {
            HashSet<int>? blockers = null;

            if (tableLock.WriteOwnerThreadId.HasValue && tableLock.WriteOwnerThreadId.Value != threadId)
            {
                (blockers ??= []).Add(tableLock.WriteOwnerThreadId.Value);
            }

            if (write)
            {
                foreach (int readerId in tableLock.ReadOwnerCounts.Keys)
                {
                    if (readerId != threadId)
                    {
                        (blockers ??= []).Add(readerId);
                    }
                }
            }

            return blockers;
        }
    }

    /// <summary>The threads a row or key lock request would wait for; null when there are none.</summary>
    private static HashSet<int>? GetRowBlockingThreads(RowLockEntry rowLock, int threadId, bool write)
    {
        lock (rowLock.OwnershipSync)
        {
            HashSet<int>? blockers = null;

            if (rowLock.WriteOwnerThreadId.HasValue && rowLock.WriteOwnerThreadId.Value != threadId)
            {
                (blockers ??= []).Add(rowLock.WriteOwnerThreadId.Value);
            }

            if (write)
            {
                foreach (int readerId in rowLock.ReadOwnerThreadIds)
                {
                    if (readerId != threadId)
                    {
                        (blockers ??= []).Add(readerId);
                    }
                }
            }

            return blockers;
        }
    }

    private static void RegisterTableReadOwner(TableLockEntry tableLock, int threadId)
    {
        lock (tableLock.OwnershipSync)
        {
            if (tableLock.ReadOwnerCounts.TryGetValue(threadId, out int count))
            {
                tableLock.ReadOwnerCounts[threadId] = count + 1;
            }
            else
            {
                tableLock.ReadOwnerCounts[threadId] = 1;
                t_heldLocks++;
            }
        }
    }

    private static bool UnregisterTableReadOwner(TableLockEntry tableLock, int threadId)
    {
        lock (tableLock.OwnershipSync)
        {
            if (!tableLock.ReadOwnerCounts.TryGetValue(threadId, out int count))
            {
                return false;
            }

            if (count > 1)
            {
                tableLock.ReadOwnerCounts[threadId] = count - 1;
                return false;
            }

            tableLock.ReadOwnerCounts.Remove(threadId);
            t_heldLocks--;
            return true;
        }
    }

    private static void RegisterTableWriteOwner(TableLockEntry tableLock, int threadId)
    {
        lock (tableLock.OwnershipSync)
        {
            // Counted once per ownership, like the read owners: a NoRecursion lock cannot be re-entered by its owner
            // (re-acquiring a write lock whose release is deferred never reaches here), so this is defensive.
            if (tableLock.WriteOwnerThreadId != threadId)
            {
                t_heldLocks++;
            }

            tableLock.WriteOwnerThreadId = threadId;
        }
    }

    private static void UnregisterTableWriteOwner(TableLockEntry tableLock, int threadId)
    {
        lock (tableLock.OwnershipSync)
        {
            if (tableLock.WriteOwnerThreadId == threadId)
            {
                tableLock.WriteOwnerThreadId = null;
                t_heldLocks--;
            }
        }
    }

    private static void RegisterRowReadOwner(RowLockEntry rowLock, int threadId)
    {
        lock (rowLock.OwnershipSync)
        {
            if (rowLock.ReadOwnerThreadIds.Add(threadId))
            {
                t_heldLocks++;
            }
        }
    }

    private static void UnregisterRowReadOwner(RowLockEntry rowLock, int threadId)
    {
        lock (rowLock.OwnershipSync)
        {
            if (rowLock.ReadOwnerThreadIds.Remove(threadId))
            {
                t_heldLocks--;
            }
        }
    }

    private static void RegisterRowWriteOwner(RowLockEntry rowLock, int threadId)
    {
        lock (rowLock.OwnershipSync)
        {
            // Counted once per ownership, like the read owners: a NoRecursion lock cannot be re-entered by its owner
            // (re-acquiring a write lock whose release is deferred never reaches here), so this is defensive.
            if (rowLock.WriteOwnerThreadId != threadId)
            {
                t_heldLocks++;
            }

            rowLock.WriteOwnerThreadId = threadId;
        }
    }

    private static void UnregisterRowWriteOwner(RowLockEntry rowLock, int threadId)
    {
        lock (rowLock.OwnershipSync)
        {
            if (rowLock.WriteOwnerThreadId == threadId)
            {
                rowLock.WriteOwnerThreadId = null;
                t_heldLocks--;
            }
        }
    }

    /// <summary>
    /// Records that <paramref name="waitingThreadId"/> is about to wait for <paramref name="lockEntry"/> and throws
    /// <see cref="DeadlockDetectedException"/> when that wait would close a cycle. Does nothing (and allocates
    /// nothing) when no other thread owns what is wanted.
    /// </summary>
    private WaitRegistration RegisterWaitIfBlocked(
        object lockEntry,
        string lockScope,
        HashSet<int>? blockingThreadIds,
        int waitingThreadId,
        bool write)
    {
        if (blockingThreadIds is null || blockingThreadIds.Count == 0)
        {
            return WaitRegistration.None;
        }

        string lockKey = DescribeLockEntry(lockEntry);
        lock (_waitForGraphSync)
        {
            HashSet<int> edges = GetOrCreateWaitEdges(waitingThreadId);
            edges.Clear();
            foreach (int threadId in blockingThreadIds)
            {
                if (threadId != waitingThreadId)
                {
                    edges.Add(threadId);
                }
            }

            if (edges.Count == 0)
            {
                RemoveWaitNode(waitingThreadId);
                return WaitRegistration.None;
            }

            _waitingScopes[waitingThreadId] = $"{lockScope} lock '{lockKey}'";
            _waitTargets[waitingThreadId] = (lockEntry, write);

            if (TryBuildWaitCycle(waitingThreadId, out _)
                && DropReleasedWaitEdges()
                && TryBuildWaitCycle(waitingThreadId, out List<int>? cycleThreadIds))
            {
                string diagnostic = BuildDeadlockDiagnostic(lockScope, lockKey, waitingThreadId, edges, cycleThreadIds!);
                RemoveWaitNode(waitingThreadId);
                throw new DeadlockDetectedException(
                    lockScope,
                    lockKey,
                    waitingThreadId,
                    edges.ToList(),
                    cycleThreadIds!,
                    diagnostic);
            }

            if (edges.Count == 0)
            {
                RemoveWaitNode(waitingThreadId);
                return WaitRegistration.None;
            }
        }

        WaitRegisteredForTesting?.Invoke();
        return new WaitRegistration(waitingThreadId, true);
    }

    private static string DescribeLockEntry(object lockEntry) => lockEntry switch
    {
        TableLockEntry table => table.Key,
        RowLockEntry row => row.Id.ToString(),
        _ => lockEntry.ToString() ?? string.Empty,
    };

    /// <summary>
    /// Removes edges to threads that no longer own what the waiter wants. An edge is recorded when a wait
    /// begins, so a waiter that has not been scheduled since its blocker released still points at that
    /// blocker, which would otherwise look like a deadlock. Returns <see langword="true"/> for use in a
    /// condition chain.
    /// </summary>
    private bool DropReleasedWaitEdges()
    {
        foreach (KeyValuePair<int, HashSet<int>> waiter in _waitForGraph)
        {
            if (_waitTargets.TryGetValue(waiter.Key, out (object Entry, bool Write) target))
            {
                HashSet<int>? owners = target.Entry switch
                {
                    RowLockEntry row => GetRowBlockingThreads(row, waiter.Key, target.Write),
                    TableLockEntry table => GetTableBlockingThreads(table, waiter.Key, target.Write),
                    _ => null,
                };

                if (owners is null)
                {
                    waiter.Value.Clear();
                }
                else
                {
                    waiter.Value.IntersectWith(owners);
                }
            }
        }

        return true;
    }

    /// <summary>
    /// Clears a wait registration as soon as the lock is acquired and returns
    /// <see cref="WaitRegistration.None"/> so the routine <c>finally</c> clear becomes a no-op.
    /// </summary>
    private WaitRegistration ClearWaitRegistrationEarly(WaitRegistration waitRegistration)
    {
        ClearWaitRegistration(waitRegistration);
        return WaitRegistration.None;
    }

    private void ClearWaitRegistration(WaitRegistration waitRegistration)
    {
        if (!waitRegistration.Active)
        {
            return;
        }

        lock (_waitForGraphSync)
        {
            RemoveWaitNode(waitRegistration.ThreadId);
        }
    }

    private bool TryBuildWaitCycle(int startThreadId, out List<int>? cycleThreadIds)
    {
        cycleThreadIds = null;
        if (!_waitForGraph.TryGetValue(startThreadId, out HashSet<int>? edges) || edges.Count == 0)
        {
            return false;
        }

        HashSet<int> visited = [startThreadId];
        List<int> path = [startThreadId];

        foreach (int next in edges)
        {
            path.Add(next);
            if (FindCycleDfs(next, startThreadId, visited, path, out cycleThreadIds))
            {
                return true;
            }

            path.RemoveAt(path.Count - 1);
        }

        return false;
    }

    private bool FindCycleDfs(
        int current,
        int target,
        HashSet<int> visited,
        List<int> path,
        out List<int>? cycleThreadIds)
    {
        if (current == target)
        {
            cycleThreadIds = [.. path];
            return true;
        }

        if (!visited.Add(current))
        {
            cycleThreadIds = null;
            return false;
        }

        if (!_waitForGraph.TryGetValue(current, out HashSet<int>? edges))
        {
            cycleThreadIds = null;
            return false;
        }

        foreach (int next in edges)
        {
            path.Add(next);
            if (FindCycleDfs(next, target, visited, path, out cycleThreadIds))
            {
                return true;
            }

            path.RemoveAt(path.Count - 1);
        }

        cycleThreadIds = null;
        return false;
    }

    private HashSet<int> GetOrCreateWaitEdges(int waitingThreadId)
    {
        if (_waitForGraph.TryGetValue(waitingThreadId, out HashSet<int>? edges))
        {
            return edges;
        }

        edges = [];
        _waitForGraph[waitingThreadId] = edges;
        return edges;
    }

    private void RemoveWaitNode(int waitingThreadId)
    {
        _waitForGraph.Remove(waitingThreadId);
        _waitingScopes.Remove(waitingThreadId);
        _waitTargets.Remove(waitingThreadId);
    }

    private string BuildDeadlockDiagnostic(
        string lockScope,
        string lockKey,
        int waitingThreadId,
        HashSet<int> blockingThreadIds,
        IReadOnlyList<int> cycleThreadIds)
    {
        string waitingOn = _waitingScopes.TryGetValue(waitingThreadId, out string? waitingScope)
            ? waitingScope
            : $"{lockScope} lock '{lockKey}'";

        string cycle = string.Join(" -> ", cycleThreadIds.Select(FormatThreadNode));
        string blockers = string.Join(", ", blockingThreadIds.OrderBy(id => id).Select(id => $"T{id}"));

        return $"Deadlock detected while acquiring {waitingOn}. Wait-for cycle: {cycle}. Blocking owners: {blockers}.";
    }

    private string FormatThreadNode(int threadId)
    {
        if (_waitingScopes.TryGetValue(threadId, out string? scope))
        {
            return $"T{threadId}({scope})";
        }

        return $"T{threadId}";
    }
}
