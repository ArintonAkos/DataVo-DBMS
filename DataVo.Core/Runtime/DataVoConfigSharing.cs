using System.Globalization;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Core.Runtime;

/// <summary>
/// Decides which configurations may share one engine instance. Every engine-affecting
/// <see cref="DataVoConfig"/> setting is captured explicitly (no reflection, so the engine stays
/// trimming/AOT-safe); a test enforces that new config properties are added here.
/// </summary>
internal static class DataVoConfigSharing
{
    /// <summary>
    /// Settings deliberately excluded from the fingerprint because a shareable (Disk/Lsm) engine never
    /// reads them: the storage backend comes from <see cref="DataVoConfig.StorageMode"/> instead.
    /// </summary>
    internal static readonly IReadOnlyList<string> IgnoredProperties =
    [
        nameof(DataVoConfig.CustomStorageEngine),
        nameof(DataVoConfig.WasmStorageEngine),
    ];

    /// <summary>
    /// Returns the canonical data directory for storage modes whose engines own on-disk state, or
    /// <see langword="null"/> when the mode keeps no shared directory (each engine stays private).
    /// </summary>
    internal static string? ResolvePersistentRoot(DataVoConfig config)
    {
        string? root = config.ResolveDataDirectory();

        return root is null ? null : TrimTrailingSeparators(Path.GetFullPath(root));
    }

    // Path.TrimEndingDirectorySeparator is unavailable on netstandard2.1; never trims a filesystem root.
    private static string TrimTrailingSeparators(string fullPath)
    {
        int rootLength = Path.GetPathRoot(fullPath)?.Length ?? 0;
        int end = fullPath.Length;
        while (end > rootLength
            && (fullPath[end - 1] == Path.DirectorySeparatorChar || fullPath[end - 1] == Path.AltDirectorySeparatorChar))
        {
            end--;
        }

        return end == fullPath.Length ? fullPath : fullPath.Substring(0, end);
    }

    /// <summary>
    /// Captures the configuration as ordered (name, value) pairs. Call it before the engine starts:
    /// engine startup may rewrite fields in place (for example hashing configured passwords).
    /// </summary>
    internal static IReadOnlyList<KeyValuePair<string, string>> Capture(DataVoConfig config)
    {
        var values = new List<KeyValuePair<string, string>>(64);

        void Add(string name, object? value) =>
            values.Add(new KeyValuePair<string, string>(name, Format(value)));

        Add(nameof(DataVoConfig.EnableAuthorization), config.EnableAuthorization);
        Add(nameof(DataVoConfig.AllowAnonymousSession), config.AllowAnonymousSession);
        Add(nameof(DataVoConfig.AnonymousRole), config.AnonymousRole);
        Add(nameof(DataVoConfig.AuthorizationUsers), FormatUsers(config.AuthorizationUsers));
        Add(nameof(DataVoConfig.StorageMode), config.StorageMode);
        Add(nameof(DataVoConfig.EnableVolcanoExecution), config.EnableVolcanoExecution);
        Add(nameof(DataVoConfig.EnableVolcanoSpillGuardrails), config.EnableVolcanoSpillGuardrails);
        Add(nameof(DataVoConfig.VolcanoSortSpillThresholdRows), config.VolcanoSortSpillThresholdRows);
        Add(nameof(DataVoConfig.VolcanoAggregateSpillThresholdRows), config.VolcanoAggregateSpillThresholdRows);
        Add(nameof(DataVoConfig.VolcanoNestedLoopJoinThresholdRows), config.VolcanoNestedLoopJoinThresholdRows);
        Add(nameof(DataVoConfig.EnableVolcanoExternalSortSpill), config.EnableVolcanoExternalSortSpill);
        Add(nameof(DataVoConfig.VolcanoExternalSortThresholdRows), config.VolcanoExternalSortThresholdRows);
        Add(nameof(DataVoConfig.VolcanoExternalSortRunSizeRows), config.VolcanoExternalSortRunSizeRows);
        Add(nameof(DataVoConfig.VolcanoExternalSortTempDirectory), config.VolcanoExternalSortTempDirectory);
        Add(nameof(DataVoConfig.EnableVolcanoExternalAggregateSpill), config.EnableVolcanoExternalAggregateSpill);
        Add(nameof(DataVoConfig.VolcanoExternalAggregateThresholdRows), config.VolcanoExternalAggregateThresholdRows);
        Add(nameof(DataVoConfig.VolcanoExternalAggregatePartitionCount), config.VolcanoExternalAggregatePartitionCount);
        Add(nameof(DataVoConfig.VolcanoExternalAggregateTempDirectory), config.VolcanoExternalAggregateTempDirectory);
        Add(nameof(DataVoConfig.VolcanoExternalAggregateAdaptivePartitioning), config.VolcanoExternalAggregateAdaptivePartitioning);
        Add(nameof(DataVoConfig.VolcanoExternalAggregateTargetRowsPerPartition), config.VolcanoExternalAggregateTargetRowsPerPartition);
        Add(nameof(DataVoConfig.VolcanoExternalAggregateMaxPartitionCount), config.VolcanoExternalAggregateMaxPartitionCount);
        Add(nameof(DataVoConfig.EnableVolcanoJoinCardinalityFeedback), config.EnableVolcanoJoinCardinalityFeedback);
        Add(nameof(DataVoConfig.EnableVolcanoJoinCardinalityFeedbackPersistence), config.EnableVolcanoJoinCardinalityFeedbackPersistence);
        Add(nameof(DataVoConfig.VolcanoJoinCardinalityFeedbackPersistenceFile), config.VolcanoJoinCardinalityFeedbackPersistenceFile);
        Add(nameof(DataVoConfig.VolcanoJoinCardinalityFeedbackMaxEntries), config.VolcanoJoinCardinalityFeedbackMaxEntries);
        Add(nameof(DataVoConfig.EnableVectorPredicateFastPath), config.EnableVectorPredicateFastPath);
        Add(nameof(DataVoConfig.VectorPredicateFastPathMinRows), config.VectorPredicateFastPathMinRows);
        Add(nameof(DataVoConfig.VectorPredicateFastPathMaxTopKRatio), config.VectorPredicateFastPathMaxTopKRatio);
        Add(nameof(DataVoConfig.VectorPredicateFastPathCandidateMultiplier), config.VectorPredicateFastPathCandidateMultiplier);
        Add(nameof(DataVoConfig.VectorPredicateFastPathMaxTopK), config.VectorPredicateFastPathMaxTopK);
        Add(nameof(DataVoConfig.EnableVectorPredicateFastPathTelemetry), config.EnableVectorPredicateFastPathTelemetry);
        Add(nameof(DataVoConfig.VectorPredicateFastPathMaxExpansionPasses), config.VectorPredicateFastPathMaxExpansionPasses);
        Add(nameof(DataVoConfig.VectorPredicateFastPathExpansionFactor), config.VectorPredicateFastPathExpansionFactor);
        Add(nameof(DataVoConfig.EnableHybridRoutingTelemetryCounters), config.EnableHybridRoutingTelemetryCounters);
        Add(nameof(DataVoConfig.EnableHybridRoutingPerQueryTelemetry), config.EnableHybridRoutingPerQueryTelemetry);
        Add(nameof(DataVoConfig.HybridRoutingTelemetrySnapshotIntervalQueries), config.HybridRoutingTelemetrySnapshotIntervalQueries);
        Add(nameof(DataVoConfig.EnableHybridOrderByAdaptiveInitialTopK), config.EnableHybridOrderByAdaptiveInitialTopK);
        Add(nameof(DataVoConfig.DiskStoragePath), ResolvePersistentRoot(config) ?? config.DiskStoragePath);
        Add(nameof(DataVoConfig.LockAcquireTimeoutMs), config.LockAcquireTimeoutMs);
        Add(nameof(DataVoConfig.SyncDiskWrites), config.SyncDiskWrites);
        Add(nameof(DataVoConfig.LsmStrictFsync), config.LsmStrictFsync);
        Add(nameof(DataVoConfig.LsmReadMode), config.LsmReadMode);
        Add(nameof(DataVoConfig.IoSchedulerMode), config.IoSchedulerMode);
        Add(nameof(DataVoConfig.EnableZeroAllocCompiledUpdate), config.EnableZeroAllocCompiledUpdate);
        Add(nameof(DataVoConfig.WalEnabled), config.WalEnabled);
        Add(nameof(DataVoConfig.WalFilePath), Path.GetFullPath(config.ResolveWalFilePath()));
        Add(nameof(DataVoConfig.WalCheckpointThreshold), config.WalCheckpointThreshold);
        Add(nameof(DataVoConfig.TransactionIdStateFilePath), Path.GetFullPath(config.ResolveTransactionIdStateFilePath()));
        Add(nameof(DataVoConfig.CheckpointStateFilePath), Path.GetFullPath(config.ResolveCheckpointStateFilePath()));
        Add(nameof(DataVoConfig.WalCheckpointIntervalMs), config.WalCheckpointIntervalMs);

        return values;
    }

    /// <summary>
    /// Returns the name of the first setting that differs between two captured configurations, or
    /// <see langword="null"/> when they are equivalent and may share one engine.
    /// </summary>
    internal static string? FindConflict(
        IReadOnlyList<KeyValuePair<string, string>> existing,
        IReadOnlyList<KeyValuePair<string, string>> requested)
    {
        for (int i = 0; i < existing.Count; i++)
        {
            if (!string.Equals(existing[i].Value, requested[i].Value, StringComparison.Ordinal))
            {
                return existing[i].Key;
            }
        }

        return null;
    }

    private static string Format(object? value) => value switch
    {
        null => "\0null",
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? string.Empty,
    };

    private static string FormatUsers(List<DataVoAuthUser> users)
    {
        var parts = new List<string>(users.Count);
        foreach (DataVoAuthUser user in users)
        {
            parts.Add(string.Join(
                "\u001f",
                user.Username,
                user.Password,
                user.PasswordHash,
                user.PasswordSalt,
                user.Role.ToString(),
                string.Join("\u001e", user.Roles)));
        }

        return string.Join("\u001d", parts);
    }
}
