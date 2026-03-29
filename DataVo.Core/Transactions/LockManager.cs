using System.Collections.Concurrent;
using System.Threading;

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
    }

    private sealed class RowLockEntry
    {
        public RowLockEntry()
        {
            Lock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
        }

        public ReaderWriterLockSlim Lock { get; }
        public int ActiveUsers;
    }

    private static readonly Lazy<LockManager> _instance = new(() => new LockManager());

    private readonly ConcurrentDictionary<string, TableLockEntry> _tableLocks =
        new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, RowLockEntry> _rowLocks =
        new(StringComparer.OrdinalIgnoreCase);
    private readonly object _tableLockLifecycleSync = new();
    private readonly object _rowLockLifecycleSync = new();
    private readonly int _lockAcquireTimeoutMs;

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

    public void AcquireReadLock(string databaseName, string tableName)
    {
        AcquireTableLock(BuildTableKey(databaseName, tableName), write: false);
    }

    public void AcquireWriteLock(string databaseName, string tableName)
    {
        AcquireTableLock(BuildTableKey(databaseName, tableName), write: true);
    }

    public void ReleaseReadLock(string databaseName, string tableName)
    {
        ReleaseTableLock(BuildTableKey(databaseName, tableName), write: false);
    }

    public void ReleaseWriteLock(string databaseName, string tableName)
    {
        ReleaseTableLock(BuildTableKey(databaseName, tableName), write: true);
    }

    public void AcquireRowReadLock(string databaseName, string tableName, long rowId)
    {
        string rowKey = BuildRowKey(databaseName, tableName, rowId);
        RowLockEntry rowLock = RetainRowLock(rowKey);
        if (!rowLock.Lock.TryEnterReadLock(_lockAcquireTimeoutMs))
        {
            ReleaseRowLock(rowKey, rowLock);
            throw new TimeoutException($"Timed out acquiring row read lock for '{rowKey}'.");
        }
    }

    public void AcquireRowWriteLock(string databaseName, string tableName, long rowId)
    {
        string rowKey = BuildRowKey(databaseName, tableName, rowId);
        RowLockEntry rowLock = RetainRowLock(rowKey);
        if (!rowLock.Lock.TryEnterWriteLock(_lockAcquireTimeoutMs))
        {
            ReleaseRowLock(rowKey, rowLock);
            throw new TimeoutException($"Timed out acquiring row write lock for '{rowKey}'.");
        }
    }

    public void ReleaseRowReadLock(string databaseName, string tableName, long rowId)
    {
        string rowKey = BuildRowKey(databaseName, tableName, rowId);
        if (!_rowLocks.TryGetValue(rowKey, out RowLockEntry? rowLock))
        {
            throw new SynchronizationLockException($"Row read lock not found for key '{rowKey}'.");
        }

        rowLock.Lock.ExitReadLock();
        ReleaseRowLock(rowKey, rowLock);
    }

    public void ReleaseRowWriteLock(string databaseName, string tableName, long rowId)
    {
        string rowKey = BuildRowKey(databaseName, tableName, rowId);
        if (!_rowLocks.TryGetValue(rowKey, out RowLockEntry? rowLock))
        {
            throw new SynchronizationLockException($"Row write lock not found for key '{rowKey}'.");
        }

        rowLock.Lock.ExitWriteLock();
        ReleaseRowLock(rowKey, rowLock);
    }

    public List<long> AcquireRowWriteLocks(string databaseName, string tableName, IEnumerable<long> rowIds)
    {
        List<long> ordered = BuildOrderedRowLockList(rowIds);

        for (int i = 0; i < ordered.Count; i++)
        {
            AcquireRowWriteLock(databaseName, tableName, ordered[i]);
        }

        return ordered;
    }

    public void ReleaseRowWriteLocks(string databaseName, string tableName, IReadOnlyList<long> rowIds)
    {
        for (int i = rowIds.Count - 1; i >= 0; i--)
        {
            ReleaseRowWriteLock(databaseName, tableName, rowIds[i]);
        }
    }

    public List<long> AcquireRowReadLocks(string databaseName, string tableName, IEnumerable<long> rowIds)
    {
        List<long> ordered = BuildOrderedRowLockList(rowIds);

        for (int i = 0; i < ordered.Count; i++)
        {
            AcquireRowReadLock(databaseName, tableName, ordered[i]);
        }

        return ordered;
    }

    public void ReleaseRowReadLocks(string databaseName, string tableName, IReadOnlyList<long> rowIds)
    {
        for (int i = rowIds.Count - 1; i >= 0; i--)
        {
            ReleaseRowReadLock(databaseName, tableName, rowIds[i]);
        }
    }

    public void AcquireReadLock(string tableKey)
    {
        AcquireTableLock(tableKey, write: false);
    }

    public void AcquireWriteLock(string tableKey)
    {
        AcquireTableLock(tableKey, write: true);
    }

    public void ReleaseReadLock(string tableKey)
    {
        ReleaseTableLock(tableKey, write: false);
    }

    public void ReleaseWriteLock(string tableKey)
    {
        ReleaseTableLock(tableKey, write: true);
    }

    private void AcquireTableLock(string tableKey, bool write)
    {
        TableLockEntry tableLock = RetainTableLock(tableKey);
        try
        {
            if (write)
            {
                if (!tableLock.Lock.TryEnterWriteLock(_lockAcquireTimeoutMs))
                {
                    throw new TimeoutException($"Timed out acquiring table write lock for '{tableKey}'.");
                }
            }
            else
            {
                if (!tableLock.Lock.TryEnterReadLock(_lockAcquireTimeoutMs))
                {
                    throw new TimeoutException($"Timed out acquiring table read lock for '{tableKey}'.");
                }
            }
        }
        catch
        {
            ReleaseTableLockEntry(tableKey, tableLock);
            throw;
        }
    }

    private void ReleaseTableLock(string tableKey, bool write)
    {
        if (!_tableLocks.TryGetValue(tableKey, out TableLockEntry? tableLock))
        {
            throw new SynchronizationLockException($"Table {(write ? "write" : "read")} lock not found for key '{tableKey}'.");
        }

        if (write)
        {
            tableLock.Lock.ExitWriteLock();
        }
        else
        {
            tableLock.Lock.ExitReadLock();
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
}