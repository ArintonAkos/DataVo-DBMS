using System.Collections.Concurrent;
using System.Threading;
using DataVo.Core.Exceptions;

namespace DataVo.Core.Transactions;

/// <summary>
/// Thread-safe singleton that manages table-scoped reader/writer locks.
/// <para>
/// Locks are keyed by the fully-qualified table identity <c>{database}.{table}</c>,
/// allowing concurrent readers while guaranteeing exclusive access for writers.
/// </para>
/// </summary>
public sealed class LockManager
{
    private const int DefaultLockAcquireTimeoutMs = 30000;

    private sealed class TableLockEntry
    {
        public TableLockEntry()
        {
            Lock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
        }

        public ReaderWriterLockSlim Lock { get; }
        public int ActiveUsers;
        public readonly object OwnershipSync = new();
        public readonly HashSet<int> ReadOwnerThreadIds = [];
        public int? WriteOwnerThreadId;
    }

    private sealed class RowLockEntry
    {
        public RowLockEntry()
        {
            Lock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
        }

        public ReaderWriterLockSlim Lock { get; }
        public int ActiveUsers;
        public readonly object OwnershipSync = new();
        public readonly HashSet<int> ReadOwnerThreadIds = [];
        public int? WriteOwnerThreadId;
    }

    private readonly record struct WaitRegistration(int ThreadId, bool Active)
    {
        public static readonly WaitRegistration None = new(0, false);
    }

    private static readonly Lazy<LockManager> _instance = new(() => new LockManager());

    private readonly ConcurrentDictionary<string, TableLockEntry> _tableLocks =
        new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, RowLockEntry> _rowLocks =
        new(StringComparer.OrdinalIgnoreCase);
    private readonly object _tableLockLifecycleSync = new();
    private readonly object _rowLockLifecycleSync = new();
    private readonly object _waitForGraphSync = new();
    private readonly int _lockAcquireTimeoutMs;
    private readonly Dictionary<int, HashSet<int>> _waitForGraph = [];
    private readonly Dictionary<int, string> _waitingScopes = [];

    /// <summary>
    /// Gets the process-wide singleton lock manager instance.
    /// </summary>
    public static LockManager Instance => _instance.Value;

    /// <summary>
    /// Creates a lock manager instance.
    /// </summary>
    /// <remarks>
    /// The legacy process-wide singleton remains available through <see cref="Instance"/>,
    /// while engine-scoped runtimes can create dedicated instances directly.
    /// </remarks>
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
        AcquireTableLock(BuildTableKey(databaseName, tableName), write: false);
    }

    /// <summary>
    /// Acquires a table-level write lock for a database/table pair.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    public void AcquireWriteLock(string databaseName, string tableName)
    {
        AcquireTableLock(BuildTableKey(databaseName, tableName), write: true);
    }

    /// <summary>
    /// Releases a table-level read lock for a database/table pair.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    public void ReleaseReadLock(string databaseName, string tableName)
    {
        ReleaseTableLock(BuildTableKey(databaseName, tableName), write: false);
    }

    /// <summary>
    /// Releases a table-level write lock for a database/table pair.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    public void ReleaseWriteLock(string databaseName, string tableName)
    {
        ReleaseTableLock(BuildTableKey(databaseName, tableName), write: true);
    }

    /// <summary>
    /// Acquires a row-level read lock.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="rowId">The row identifier.</param>
    public void AcquireRowReadLock(string databaseName, string tableName, long rowId)
    {
        string rowKey = BuildRowKey(databaseName, tableName, rowId);
        RowLockEntry rowLock = RetainRowLock(rowKey);
        int threadId = Environment.CurrentManagedThreadId;
        WaitRegistration waitRegistration = WaitRegistration.None;

        try
        {
            waitRegistration = RegisterWaitIfBlocked(rowKey, "row read", GetRowBlockingThreads(rowLock, threadId, write: false), threadId);

            if (!rowLock.Lock.TryEnterReadLock(_lockAcquireTimeoutMs))
            {
                throw new TimeoutException($"Timed out acquiring row read lock for '{rowKey}'.");
            }

            RegisterRowReadOwner(rowLock, threadId);
        }
        catch
        {
            ReleaseRowLock(rowKey, rowLock);
            throw;
        }
        finally
        {
            ClearWaitRegistration(waitRegistration);
        }
    }

    /// <summary>
    /// Acquires a row-level write lock.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="rowId">The row identifier.</param>
    public void AcquireRowWriteLock(string databaseName, string tableName, long rowId)
    {
        string rowKey = BuildRowKey(databaseName, tableName, rowId);
        RowLockEntry rowLock = RetainRowLock(rowKey);
        int threadId = Environment.CurrentManagedThreadId;
        WaitRegistration waitRegistration = WaitRegistration.None;

        try
        {
            waitRegistration = RegisterWaitIfBlocked(rowKey, "row write", GetRowBlockingThreads(rowLock, threadId, write: true), threadId);

            if (!rowLock.Lock.TryEnterWriteLock(_lockAcquireTimeoutMs))
            {
                throw new TimeoutException($"Timed out acquiring row write lock for '{rowKey}'.");
            }

            RegisterRowWriteOwner(rowLock, threadId);
        }
        catch
        {
            ReleaseRowLock(rowKey, rowLock);
            throw;
        }
        finally
        {
            ClearWaitRegistration(waitRegistration);
        }
    }

    /// <summary>
    /// Releases a row-level read lock.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="rowId">The row identifier.</param>
    public void ReleaseRowReadLock(string databaseName, string tableName, long rowId)
    {
        string rowKey = BuildRowKey(databaseName, tableName, rowId);
        if (!_rowLocks.TryGetValue(rowKey, out RowLockEntry? rowLock))
        {
            throw new InvalidOperationException($"Row read lock not found for key '{rowKey}'.");
        }

        rowLock.Lock.ExitReadLock();
        UnregisterRowReadOwner(rowLock, Environment.CurrentManagedThreadId);
        ReleaseRowLock(rowKey, rowLock);
    }

    /// <summary>
    /// Releases a row-level write lock.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="rowId">The row identifier.</param>
    public void ReleaseRowWriteLock(string databaseName, string tableName, long rowId)
    {
        string rowKey = BuildRowKey(databaseName, tableName, rowId);
        if (!_rowLocks.TryGetValue(rowKey, out RowLockEntry? rowLock))
        {
            throw new InvalidOperationException($"Row write lock not found for key '{rowKey}'.");
        }

        rowLock.Lock.ExitWriteLock();
        UnregisterRowWriteOwner(rowLock, Environment.CurrentManagedThreadId);
        ReleaseRowLock(rowKey, rowLock);
    }

    /// <summary>
    /// Acquires row-level write locks in deterministic order.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="rowIds">Row identifiers to lock.</param>
    /// <returns>The ordered lock acquisition list.</returns>
    public List<long> AcquireRowWriteLocks(string databaseName, string tableName, IEnumerable<long> rowIds)
    {
        List<long> ordered = BuildOrderedRowLockList(rowIds);

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
    public void ReleaseRowWriteLocks(string databaseName, string tableName, IReadOnlyList<long> rowIds)
    {
        for (int i = rowIds.Count - 1; i >= 0; i--)
        {
            ReleaseRowWriteLock(databaseName, tableName, rowIds[i]);
        }
    }

    /// <summary>
    /// Acquires row-level read locks in deterministic order.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="rowIds">Row identifiers to lock.</param>
    /// <returns>The ordered lock acquisition list.</returns>
    public List<long> AcquireRowReadLocks(string databaseName, string tableName, IEnumerable<long> rowIds)
    {
        List<long> ordered = BuildOrderedRowLockList(rowIds);

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
    public void ReleaseRowReadLocks(string databaseName, string tableName, IReadOnlyList<long> rowIds)
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
        AcquireTableLock(tableKey, write: false);
    }

    /// <summary>
    /// Acquires a table-scoped write lock using a pre-composed lock key in the form <c>{database}.{table}</c>.
    /// </summary>
    public void AcquireWriteLock(string tableKey)
    {
        AcquireTableLock(tableKey, write: true);
    }

    /// <summary>
    /// Releases a table-scoped read lock acquired through the table-key overloads.
    /// </summary>
    public void ReleaseReadLock(string tableKey)
    {
        ReleaseTableLock(tableKey, write: false);
    }

    /// <summary>
    /// Releases a table-scoped write lock acquired through the table-key overloads.
    /// </summary>
    public void ReleaseWriteLock(string tableKey)
    {
        ReleaseTableLock(tableKey, write: true);
    }

    private void AcquireTableLock(string tableKey, bool write)
    {
        TableLockEntry tableLock = RetainTableLock(tableKey);
        int threadId = Environment.CurrentManagedThreadId;
        WaitRegistration waitRegistration = WaitRegistration.None;

        try
        {
            waitRegistration = RegisterWaitIfBlocked(tableKey, write ? "table write" : "table read", GetTableBlockingThreads(tableLock, threadId, write), threadId);

            if (write)
            {
                if (!tableLock.Lock.TryEnterWriteLock(_lockAcquireTimeoutMs))
                {
                    throw new TimeoutException($"Timed out acquiring table write lock for '{tableKey}'.");
                }

                RegisterTableWriteOwner(tableLock, threadId);
            }
            else
            {
                if (!tableLock.Lock.TryEnterReadLock(_lockAcquireTimeoutMs))
                {
                    throw new TimeoutException($"Timed out acquiring table read lock for '{tableKey}'.");
                }

                RegisterTableReadOwner(tableLock, threadId);
            }
        }
        catch
        {
            ReleaseTableLockEntry(tableKey, tableLock);
            throw;
        }
        finally
        {
            ClearWaitRegistration(waitRegistration);
        }
    }

    private void ReleaseTableLock(string tableKey, bool write)
    {
        if (!_tableLocks.TryGetValue(tableKey, out TableLockEntry? tableLock))
        {
            throw new InvalidOperationException($"Table {(write ? "write" : "read")} lock not found for key '{tableKey}'.");
        }

        if (write)
        {
            tableLock.Lock.ExitWriteLock();
            UnregisterTableWriteOwner(tableLock, Environment.CurrentManagedThreadId);
        }
        else
        {
            tableLock.Lock.ExitReadLock();
            UnregisterTableReadOwner(tableLock, Environment.CurrentManagedThreadId);
        }

        ReleaseTableLockEntry(tableKey, tableLock);
    }

    private TableLockEntry RetainTableLock(string tableKey)
    {
        lock (_tableLockLifecycleSync)
        {
            TableLockEntry tableLock = _tableLocks.GetOrAdd(tableKey, _ => new TableLockEntry());
            tableLock.ActiveUsers++;
            return tableLock;
        }
    }

    private void ReleaseTableLockEntry(string tableKey, TableLockEntry tableLock)
    {
        lock (_tableLockLifecycleSync)
        {
            if (!_tableLocks.TryGetValue(tableKey, out TableLockEntry? current)
                || !ReferenceEquals(current, tableLock))
            {
                return;
            }

            current.ActiveUsers--;
            if (current.ActiveUsers > 0)
            {
                return;
            }

            _tableLocks.TryRemove(tableKey, out _);
            current.Lock.Dispose();
        }
    }

    private RowLockEntry RetainRowLock(string rowKey)
    {
        lock (_rowLockLifecycleSync)
        {
            RowLockEntry rowLock = _rowLocks.GetOrAdd(rowKey, _ => new RowLockEntry());
            rowLock.ActiveUsers++;
            return rowLock;
        }
    }

    private void ReleaseRowLock(string rowKey, RowLockEntry rowLock)
    {
        lock (_rowLockLifecycleSync)
        {
            if (!_rowLocks.TryGetValue(rowKey, out RowLockEntry? current)
                || !ReferenceEquals(current, rowLock))
            {
                return;
            }

            current.ActiveUsers--;
            if (current.ActiveUsers > 0)
            {
                return;
            }

            _rowLocks.TryRemove(rowKey, out _);
            current.Lock.Dispose();
        }
    }

    private static List<long> BuildOrderedRowLockList(IEnumerable<long> rowIds)
    {
        return rowIds
            .Where(id => id > 0)
            .Distinct()
            .OrderBy(id => id)
            .ToList();
    }

    private static string BuildTableKey(string databaseName, string tableName)
    {
        return $"{databaseName}.{tableName}";
    }

    private static string BuildRowKey(string databaseName, string tableName, long rowId)
    {
        return $"{databaseName}.{tableName}#row:{rowId}";
    }

    private static IReadOnlyCollection<int> GetTableBlockingThreads(TableLockEntry tableLock, int threadId, bool write)
    {
        lock (tableLock.OwnershipSync)
        {
            HashSet<int> blockers = [];

            if (tableLock.WriteOwnerThreadId.HasValue && tableLock.WriteOwnerThreadId.Value != threadId)
            {
                blockers.Add(tableLock.WriteOwnerThreadId.Value);
            }

            if (write)
            {
                foreach (int readerId in tableLock.ReadOwnerThreadIds)
                {
                    if (readerId != threadId)
                    {
                        blockers.Add(readerId);
                    }
                }
            }

            return blockers;
        }
    }

    private static IReadOnlyCollection<int> GetRowBlockingThreads(RowLockEntry rowLock, int threadId, bool write)
    {
        lock (rowLock.OwnershipSync)
        {
            HashSet<int> blockers = [];

            if (rowLock.WriteOwnerThreadId.HasValue && rowLock.WriteOwnerThreadId.Value != threadId)
            {
                blockers.Add(rowLock.WriteOwnerThreadId.Value);
            }

            if (write)
            {
                foreach (int readerId in rowLock.ReadOwnerThreadIds)
                {
                    if (readerId != threadId)
                    {
                        blockers.Add(readerId);
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
            tableLock.ReadOwnerThreadIds.Add(threadId);
        }
    }

    private static void UnregisterTableReadOwner(TableLockEntry tableLock, int threadId)
    {
        lock (tableLock.OwnershipSync)
        {
            tableLock.ReadOwnerThreadIds.Remove(threadId);
        }
    }

    private static void RegisterTableWriteOwner(TableLockEntry tableLock, int threadId)
    {
        lock (tableLock.OwnershipSync)
        {
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
            }
        }
    }

    private static void RegisterRowReadOwner(RowLockEntry rowLock, int threadId)
    {
        lock (rowLock.OwnershipSync)
        {
            rowLock.ReadOwnerThreadIds.Add(threadId);
        }
    }

    private static void UnregisterRowReadOwner(RowLockEntry rowLock, int threadId)
    {
        lock (rowLock.OwnershipSync)
        {
            rowLock.ReadOwnerThreadIds.Remove(threadId);
        }
    }

    private static void RegisterRowWriteOwner(RowLockEntry rowLock, int threadId)
    {
        lock (rowLock.OwnershipSync)
        {
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
            }
        }
    }

    private WaitRegistration RegisterWaitIfBlocked(
        string lockKey,
        string lockScope,
        IReadOnlyCollection<int> blockingThreadIds,
        int waitingThreadId)
    {
        if (blockingThreadIds.Count == 0)
        {
            return WaitRegistration.None;
        }

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

            if (TryBuildWaitCycle(waitingThreadId, out List<int>? cycleThreadIds))
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

            return new WaitRegistration(waitingThreadId, true);
        }
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