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

    public static LockManager Instance => _instance.Value;

    private LockManager() { }

    public void AcquireReadLock(string databaseName, string tableName)
    {
        GetLock(databaseName, tableName).EnterReadLock();
    }

    public void AcquireWriteLock(string databaseName, string tableName)
    {
        GetLock(databaseName, tableName).EnterWriteLock();
    }

    public void ReleaseReadLock(string databaseName, string tableName)
    {
        GetLock(databaseName, tableName).ExitReadLock();
    }

    public void ReleaseWriteLock(string databaseName, string tableName)
    {
        GetLock(databaseName, tableName).ExitWriteLock();
    }

    public void AcquireReadLock(string tableKey)
    {
        GetLock(tableKey).EnterReadLock();
    }

    public void AcquireWriteLock(string tableKey)
    {
        GetLock(tableKey).EnterWriteLock();
    }

    public void ReleaseReadLock(string tableKey)
    {
        GetLock(tableKey).ExitReadLock();
    }

    public void ReleaseWriteLock(string tableKey)
    {
        GetLock(tableKey).ExitWriteLock();
    }

    private ReaderWriterLockSlim GetLock(string databaseName, string tableName)
    {
        return GetLock(BuildTableKey(databaseName, tableName));
    }

    private ReaderWriterLockSlim GetLock(string tableKey)
    {
        return _tableLocks.GetOrAdd(tableKey, _ => new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion));
    }

    private static string BuildTableKey(string databaseName, string tableName)
    {
        return $"{databaseName}.{tableName}";
    }
}