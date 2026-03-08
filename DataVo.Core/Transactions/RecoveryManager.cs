using DataVo.Core.Logging;
using DataVo.Core.Parser.Transactions;
using DataVo.Core.Runtime;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Core.Transactions;

/// <summary>
/// Replays durable WAL entries that were not checkpointed before the previous process stopped.
/// </summary>
/// <remarks>
/// Recovery runs during storage initialization for disk-backed configurations with WAL enabled.
/// Each uncheckpointed transaction is replayed under table-level write locks to preserve the same
/// serialization guarantees used during normal commits.
/// </remarks>
/// <example>
/// <code>
/// var recoveryManager = new RecoveryManager(config);
/// recoveryManager.Recover();
/// </code>
/// </example>
public sealed class RecoveryManager(DataVoConfig config, DataVoEngine? engine = null)
{
    /// <summary>
    /// The active engine configuration used to decide whether recovery should run.
    /// </summary>
    private readonly DataVoConfig _config = config;
    private readonly DataVoEngine _engine = engine ?? DataVoEngine.Current();

    /// <summary>
    /// Reads persisted WAL entries from disk.
    /// </summary>
    private readonly WalReader _reader = new(config);

    /// <summary>
    /// Updates WAL state after entries are replayed successfully.
    /// </summary>
    private readonly WalWriter _writer = new(config);

    /// <summary>
    /// Replays every uncheckpointed WAL entry and then prunes checkpointed entries when possible.
    /// </summary>
    public void Recover()
    {
        if (!ShouldRecover())
        {
            return;
        }

        List<WalEntry> entries = _reader.ReadUncheckpointed();

        foreach (var entry in entries)
        {
            RecoverEntry(entry);
        }

        _writer.PruneCheckpointedEntries(forceIfAllCheckpointed: true);
    }

    /// <summary>
    /// Determines whether the current configuration requires WAL recovery.
    /// </summary>
    /// <returns><c>true</c> when recovery should run; otherwise, <c>false</c>.</returns>
    private bool ShouldRecover()
    {
        return _config.StorageMode == StorageMode.Disk && _config.WalEnabled;
    }

    /// <summary>
    /// Replays one WAL entry while holding write locks for every affected table.
    /// </summary>
    /// <param name="entry">The entry to replay.</param>
    private void RecoverEntry(WalEntry entry)
    {
        List<string> lockedTables = GetAffectedTables(entry);
        AcquireWriteLocks(entry.DatabaseName, lockedTables);

        try
        {
            Commit.FlushContext(entry.ToTransactionContext(), entry.DatabaseName, _engine);
            _writer.MarkCheckpointed(entry.TransactionId);
            Logger.Info($"Recovered WAL transaction {entry.TransactionId}.");
        }
        finally
        {
            ReleaseWriteLocks(entry.DatabaseName, lockedTables);
        }
    }

    /// <summary>
    /// Collects the distinct tables touched by a WAL entry in deterministic order.
    /// </summary>
    /// <param name="entry">The entry being recovered.</param>
    /// <returns>The ordered list of affected table names.</returns>
    private static List<string> GetAffectedTables(WalEntry entry)
    {
        return [.. entry.Operations
            .Select(operation => operation.TableName)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(table => table, StringComparer.OrdinalIgnoreCase)];
    }

    /// <summary>
    /// Acquires write locks for the specified tables.
    /// </summary>
    /// <param name="databaseName">The database containing the locked tables.</param>
    /// <param name="tableNames">The tables that must be protected during replay.</param>
    private void AcquireWriteLocks(string databaseName, List<string> tableNames)
    {
        var locks = _engine.LockManager;

        foreach (string tableName in tableNames)
        {
            locks.AcquireWriteLock(databaseName, tableName);
        }
    }

    /// <summary>
    /// Releases write locks in reverse order.
    /// </summary>
    /// <param name="databaseName">The database containing the locked tables.</param>
    /// <param name="tableNames">The tables whose locks should be released.</param>
    private void ReleaseWriteLocks(string databaseName, List<string> tableNames)
    {
        var locks = _engine.LockManager;

        for (int i = tableNames.Count - 1; i >= 0; i--)
        {
            locks.ReleaseWriteLock(databaseName, tableNames[i]);
        }
    }
}
