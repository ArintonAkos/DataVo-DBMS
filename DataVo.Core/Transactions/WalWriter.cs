using DataVo.Core.StorageEngine.Config;

namespace DataVo.Core.Transactions;

/// <summary>
/// Persists and maintains write-ahead log entries on disk.
/// </summary>
/// <remarks>
/// This class appends committed transactions, marks entries as checkpointed after durable flush,
/// and prunes the log when configured thresholds are reached.
/// </remarks>
/// <example>
/// <code>
/// var writer = new WalWriter(config);
/// writer.Append(entry);
/// writer.MarkCheckpointed(entry.TransactionId);
/// </code>
/// </example>
public sealed class WalWriter(DataVoConfig config)
{
    /// <summary>
    /// The active WAL configuration.
    /// </summary>
    private readonly DataVoConfig _config = config;

    /// <summary>
    /// Provides synchronized access to the physical WAL file.
    /// </summary>
    private readonly WalFileStore _fileStore = new(config.ResolveWalFilePath());

    /// <summary>
    /// Appends a transaction entry to the WAL and forces it to disk.
    /// </summary>
    /// <param name="entry">The entry to append.</param>
    public void Append(WalEntry entry)
    {
        _fileStore.AppendEntry(entry);
    }

    /// <summary>
    /// Marks the specified transaction as checkpointed and prunes the WAL if needed.
    /// </summary>
    /// <param name="transactionId">The committed transaction identifier to mark.</param>
    public void MarkCheckpointed(Guid transactionId)
    {
        _fileStore.ExecuteLocked(() =>
        {
            List<WalEntry> entries = _fileStore.ReadEntries();
            if (!TryMarkEntryCheckpointed(entries, transactionId))
            {
                return;
            }

            _fileStore.RewriteEntries(entries);
            PruneCheckpointedEntriesCore(entries, forceIfAllCheckpointed: false);
        });
    }

    /// <summary>
    /// Removes checkpointed entries when pruning rules allow it.
    /// </summary>
    /// <param name="forceIfAllCheckpointed">
    /// When <c>true</c>, the WAL is fully truncated as soon as every entry is checkpointed.
    /// </param>
    public void PruneCheckpointedEntries(bool forceIfAllCheckpointed)
    {
        _fileStore.ExecuteLocked(() =>
        {
            List<WalEntry> entries = _fileStore.ReadEntries();
            PruneCheckpointedEntriesCore(entries, forceIfAllCheckpointed);
        });
    }

    /// <summary>
    /// Prunes checkpointed entries according to the configured threshold and the supplied policy.
    /// </summary>
    /// <param name="entries">The current in-memory WAL entries.</param>
    /// <param name="forceIfAllCheckpointed">Whether an all-checkpointed log should be truncated immediately.</param>
    private void PruneCheckpointedEntriesCore(List<WalEntry> entries, bool forceIfAllCheckpointed)
    {
        if (entries.Count == 0)
        {
            _fileStore.DeleteIfExists();
            return;
        }

        if (!ShouldPrune(entries, forceIfAllCheckpointed))
        {
            return;
        }

        _fileStore.RewriteEntries(GetRemainingEntries(entries));
    }

    /// <summary>
    /// Marks a specific in-memory entry as checkpointed.
    /// </summary>
    /// <param name="entries">The candidate entries.</param>
    /// <param name="transactionId">The transaction identifier to update.</param>
    /// <returns><c>true</c> if a matching entry was found; otherwise, <c>false</c>.</returns>
    private static bool TryMarkEntryCheckpointed(List<WalEntry> entries, Guid transactionId)
    {
        WalEntry? entry = entries.LastOrDefault(item => item.TransactionId == transactionId);
        if (entry == null)
        {
            return false;
        }

        entry.IsCheckpointed = true;
        return true;
    }

    /// <summary>
    /// Determines whether the WAL should be pruned for the current set of entries.
    /// </summary>
    /// <param name="entries">The current in-memory WAL entries.</param>
    /// <param name="forceIfAllCheckpointed">Whether a fully checkpointed WAL must be truncated immediately.</param>
    /// <returns><c>true</c> if the file should be pruned; otherwise, <c>false</c>.</returns>
    private bool ShouldPrune(List<WalEntry> entries, bool forceIfAllCheckpointed)
    {
        bool allCheckpointed = entries.All(IsCheckpointed);
        bool thresholdReached = entries.Count >= Math.Max(1, _config.WalCheckpointThreshold);

        return thresholdReached || (forceIfAllCheckpointed && allCheckpointed);
    }

    /// <summary>
    /// Returns only WAL entries that still need to remain in the file after pruning.
    /// </summary>
    /// <param name="entries">The current in-memory WAL entries.</param>
    /// <returns>The uncheckpointed entries that should remain persisted.</returns>
    private static List<WalEntry> GetRemainingEntries(List<WalEntry> entries)
    {
        return [.. entries.Where(entry => !entry.IsCheckpointed)];
    }

    /// <summary>
    /// Determines whether the supplied entry has already been checkpointed.
    /// </summary>
    /// <param name="entry">The entry to inspect.</param>
    /// <returns><c>true</c> if the entry is checkpointed; otherwise, <c>false</c>.</returns>
    private static bool IsCheckpointed(WalEntry entry)
    {
        return entry.IsCheckpointed;
    }
}
