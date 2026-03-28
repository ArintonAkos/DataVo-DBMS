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
    private static readonly Lazy<LockManager> _instance = new(() => new LockManager());

    private readonly ConcurrentDictionary<string, ReaderWriterLockSlim> _tableLocks =
        new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, ReaderWriterLockSlim> _rowLocks =
        new(StringComparer.OrdinalIgnoreCase);

    public static LockManager Instance => _instance.Value;

    /// <summary>
    /// Creates a lock manager instance.
    /// </summary>
    /// <remarks>
    /// The legacy process-wide singleton remains available through <see cref="Instance"/>,
    /// while engine-scoped runtimes can create dedicated instances directly.
    /// </remarks>
    public LockManager() { }

    public void AcquireReadLock(string databaseName, string tableName)
    {
        GetTableLock(databaseName, tableName).EnterReadLock();
    }

    public void AcquireWriteLock(string databaseName, string tableName)
    {
        GetTableLock(databaseName, tableName).EnterWriteLock();
    }

    public void ReleaseReadLock(string databaseName, string tableName)
    {
        GetTableLock(databaseName, tableName).ExitReadLock();
    }

    public void ReleaseWriteLock(string databaseName, string tableName)
    {
        GetTableLock(databaseName, tableName).ExitWriteLock();
    }

    public void AcquireRowReadLock(string databaseName, string tableName, long rowId)
    {
        GetRowLock(databaseName, tableName, rowId).EnterReadLock();
    }

    public void AcquireRowWriteLock(string databaseName, string tableName, long rowId)
    {
        GetRowLock(databaseName, tableName, rowId).EnterWriteLock();
    }

    public void ReleaseRowReadLock(string databaseName, string tableName, long rowId)
    {
        GetRowLock(databaseName, tableName, rowId).ExitReadLock();
    }

    public void ReleaseRowWriteLock(string databaseName, string tableName, long rowId)
    {
        GetRowLock(databaseName, tableName, rowId).ExitWriteLock();
    }

    public List<long> AcquireRowWriteLocks(string databaseName, string tableName, IEnumerable<long> rowIds)
    {
        List<long> ordered = rowIds
            .Distinct()
            .OrderBy(id => id)
            .ToList();

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

    public void AcquireReadLock(string tableKey)
    {
        GetTableLock(tableKey).EnterReadLock();
    }

    public void AcquireWriteLock(string tableKey)
    {
        GetTableLock(tableKey).EnterWriteLock();
    }

    public void ReleaseReadLock(string tableKey)
    {
        GetTableLock(tableKey).ExitReadLock();
    }

    public void ReleaseWriteLock(string tableKey)
    {
        GetTableLock(tableKey).ExitWriteLock();
    }

    private ReaderWriterLockSlim GetTableLock(string databaseName, string tableName)
    {
        return GetTableLock(BuildTableKey(databaseName, tableName));
    }

    private ReaderWriterLockSlim GetTableLock(string tableKey)
    {
        return _tableLocks.GetOrAdd(tableKey, _ => new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion));
    }

    private ReaderWriterLockSlim GetRowLock(string databaseName, string tableName, long rowId)
    {
        string rowKey = BuildRowKey(databaseName, tableName, rowId);
        return _rowLocks.GetOrAdd(rowKey, _ => new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion));
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