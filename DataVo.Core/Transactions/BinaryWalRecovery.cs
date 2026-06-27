using DataVo.Core.Logging;
using DataVo.Core.Parser.Transactions;
using DataVo.Core.Runtime;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Core.Transactions;

/// <summary>
/// Startup recovery for the binary write-ahead log used by
/// <see cref="IoSchedulerMode.GroupCommit"/>. It scans the durable <c>datavo.wal</c>, validates each
/// frame, and re-applies every committed frame whose LSN is newer than the persisted checkpoint —
/// reconstructing the data-file effects that a crash may have left only in the log.
/// </summary>
/// <remarks>
/// Recovery uses the same <see cref="Commit.FlushContext"/> replay path as the JSON recovery manager, so
/// index maintenance and MVCC version registration stay identical to a normal commit. Frames at or below
/// the checkpoint LSN are skipped because their effects are already durable in the <c>.dat</c> files,
/// which keeps repeated startups idempotent.
/// </remarks>
public sealed class BinaryWalRecovery
{
    private readonly DataVoConfig _config;
    private readonly DataVoEngine _engine;
    private readonly WalFileStore _walStore;
    private readonly CheckpointStateStore _checkpointStore;

    /// <summary>
    /// Initializes binary WAL recovery for the supplied configuration and engine.
    /// </summary>
    /// <param name="config">The active engine configuration.</param>
    /// <param name="engine">The engine whose data files and indexes recovery replays into.</param>
    public BinaryWalRecovery(DataVoConfig config, DataVoEngine engine)
    {
        _config = config;
        _engine = engine;
        _walStore = new WalFileStore(config.ResolveWalFilePath());
        _checkpointStore = new CheckpointStateStore(config);
    }

    /// <summary>
    /// Replays the uncheckpointed tail of the binary WAL.
    /// </summary>
    /// <returns>
    /// The highest LSN now reflected in the data files: the highest replayed frame LSN, or the existing
    /// checkpoint LSN when nothing required replay. The caller checkpoints through this value so the
    /// replayed effects become durable and the WAL prefix can be pruned.
    /// </returns>
    public long Recover()
    {
        if (!DataVoEngine.UsesGroupCommitWal(_config))
        {
            return 0;
        }

        long checkpointLsn = _checkpointStore.ReadCheckpointLsn();
        List<WalFrameRecord> frames = _walStore.ReadBinaryFrames();

        long maxLsn = checkpointLsn;
        long maxTransactionId = 0;
        var replayable = new List<WalEntry>();

        foreach (WalFrameRecord record in frames)
        {
            if (record.Header.Lsn <= checkpointLsn || record.Header.OpType != WalFrameOperationType.TxnCommit)
            {
                continue;
            }

            WalEntry? entry = WalFileStore.DeserializeWalEntryPayload(record.Payload);
            if (entry == null)
            {
                continue;
            }

            replayable.Add(entry);
            maxLsn = Math.Max(maxLsn, record.Header.Lsn);
            maxTransactionId = Math.Max(maxTransactionId, entry.MvccTransactionId);
        }

        if (replayable.Count == 0)
        {
            return checkpointLsn;
        }

        if (maxTransactionId > 0)
        {
            _engine.TransactionIdAllocator.RestoreHighWaterMark(maxTransactionId + 1);
        }

        foreach (WalEntry entry in replayable)
        {
            RecoverEntry(entry);
        }

        return maxLsn;
    }

    private void RecoverEntry(WalEntry entry)
    {
        List<string> lockedTables = GetAffectedTables(entry);
        AcquireWriteLocks(entry.DatabaseName, lockedTables);

        try
        {
            Commit.FlushContext(entry.ToTransactionContext(), entry.DatabaseName, _engine);
            Logger.Info($"Recovered binary WAL transaction {entry.TransactionId}.");
        }
        finally
        {
            ReleaseWriteLocks(entry.DatabaseName, lockedTables);
        }
    }

    private static List<string> GetAffectedTables(WalEntry entry)
    {
        return [.. entry.Operations
            .Select(operation => operation.TableName)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(table => table, StringComparer.OrdinalIgnoreCase)];
    }

    private void AcquireWriteLocks(string databaseName, List<string> tableNames)
    {
        foreach (string tableName in tableNames)
        {
            _engine.LockManager.AcquireWriteLock(databaseName, tableName);
        }
    }

    private void ReleaseWriteLocks(string databaseName, List<string> tableNames)
    {
        for (int i = tableNames.Count - 1; i >= 0; i--)
        {
            _engine.LockManager.ReleaseWriteLock(databaseName, tableNames[i]);
        }
    }
}
