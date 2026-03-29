using DataVo.Core.Runtime.Security;

namespace DataVo.Core.StorageEngine.Config;

/// <summary>
/// Defines a runtime user credential/role entry used by engine authorization.
/// </summary>
public sealed class DataVoAuthUser
{
    /// <summary>
    /// Gets or sets the unique login name.
    /// </summary>
    public string Username { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the plaintext password used for bootstrap configuration.
    /// Runtime initialization converts this to hash+salt and clears plaintext.
    /// </summary>
    public string Password { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the persisted password hash.
    /// </summary>
    public string PasswordHash { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the persisted password salt.
    /// </summary>
    public string PasswordSalt { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the legacy primary role value for compatibility.
    /// </summary>
    public DatabaseRole Role { get; set; } = DatabaseRole.ReadWrite;

    /// <summary>
    /// Gets or sets explicit role memberships assigned to the user.
    /// </summary>
    public List<string> Roles { get; set; } = [];
}

/// <summary>
/// Defines the physical storage modes supported by the engine.
/// </summary>
public enum StorageMode
{
    /// <summary>Stores data only in process memory.</summary>
    InMemory,

    /// <summary>Stores data in on-disk table files.</summary>
    Disk,

    /// <summary>Stores data in a browser/WASM backend (intended for OPFS-capable engines).</summary>
    Wasm,

    /// <summary>Stores data using a custom-provided IStorageEngine.</summary>
    Custom
}

/// <summary>
/// Represents the runtime configuration used to initialize a <c>DataVo</c> engine instance.
/// </summary>
/// <example>
/// <code>
/// var config = new DataVoConfig
/// {
///     StorageMode = StorageMode.Disk,
///     DiskStoragePath = "./demo-data",
///     WalEnabled = true,
///     WalFilePath = "demo.wal"
/// };
/// </code>
/// </example>
public class DataVoConfig
{
    /// <summary>
    /// Gets or sets a value indicating whether runtime authorization is enabled.
    /// </summary>
    public bool EnableAuthorization { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether sessions without explicit login are allowed.
    /// </summary>
    public bool AllowAnonymousSession { get; set; } = true;

    /// <summary>
    /// Gets or sets the role assigned to unauthenticated sessions when anonymous access is enabled.
    /// </summary>
    public DatabaseRole AnonymousRole { get; set; } = DatabaseRole.ReadOnly;

    /// <summary>
    /// Gets or sets configured credential entries available to <c>AuthenticateSession</c>.
    /// </summary>
    public List<DataVoAuthUser> AuthorizationUsers { get; set; } = [];

    /// <summary>
    /// Gets or sets the active storage mode.
    /// </summary>
    public StorageMode StorageMode { get; set; } = StorageMode.InMemory;

    /// <summary>
    /// Gets or sets a custom payload storage engine. 
    /// Required if StorageMode is set to Custom.
    /// </summary>
    public IStorageEngine? CustomStorageEngine { get; set; }

    /// <summary>
    /// Gets or sets a WASM/browser payload storage engine (for example OPFS-backed).
    /// Used when <see cref="StorageMode"/> is <see cref="StorageMode.Wasm"/>.
    /// When null, the runtime falls back to an in-memory backend.
    /// </summary>
    public IStorageEngine? WasmStorageEngine { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether eligible SELECT paths should use Volcano-style streaming operators.
    /// Defaults to <see langword="false"/> for compatibility.
    /// </summary>
    public bool EnableVolcanoExecution { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether Volcano planner should apply spill guardrails.
    /// When enabled, large estimated intermediates avoid in-memory Volcano sort/aggregate pushdown.
    /// </summary>
    public bool EnableVolcanoSpillGuardrails { get; set; } = true;

    /// <summary>
    /// Gets or sets the estimated row threshold for Volcano sort pushdown.
    /// A value less than or equal to zero disables this guardrail.
    /// </summary>
    public int VolcanoSortSpillThresholdRows { get; set; } = 20000;

    /// <summary>
    /// Gets or sets the estimated row threshold for Volcano aggregate pushdown.
    /// A value less than or equal to zero disables this guardrail.
    /// </summary>
    public int VolcanoAggregateSpillThresholdRows { get; set; } = 20000;

    /// <summary>
    /// Gets or sets the right-side row-count threshold for choosing nested-loop inner join in Volcano plans.
    /// When the estimated/build side is above this threshold, hash join is preferred.
    /// </summary>
    public int VolcanoNestedLoopJoinThresholdRows { get; set; } = 128;

    /// <summary>
    /// Gets or sets a value indicating whether Volcano sort operators may spill to temporary runs and merge them.
    /// </summary>
    public bool EnableVolcanoExternalSortSpill { get; set; }

    /// <summary>
    /// Gets or sets the row threshold after which Volcano sort uses external run generation and merge.
    /// A value less than or equal to zero disables threshold-based spill triggering.
    /// </summary>
    public int VolcanoExternalSortThresholdRows { get; set; } = 50000;

    /// <summary>
    /// Gets or sets the run size used during external sort run generation.
    /// </summary>
    public int VolcanoExternalSortRunSizeRows { get; set; } = 5000;

    /// <summary>
    /// Gets or sets an optional directory path for external sort temporary run files.
    /// When null or empty, the process temp directory is used.
    /// </summary>
    public string? VolcanoExternalSortTempDirectory { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether Volcano hash aggregate may spill to partition files and merge/reduce them.
    /// </summary>
    public bool EnableVolcanoExternalAggregateSpill { get; set; }

    /// <summary>
    /// Gets or sets the row threshold after which Volcano aggregate uses external partition spill.
    /// A value less than or equal to zero disables threshold-based spill triggering.
    /// </summary>
    public int VolcanoExternalAggregateThresholdRows { get; set; } = 50000;

    /// <summary>
    /// Gets or sets the number of hash partitions used during external aggregate spill.
    /// </summary>
    public int VolcanoExternalAggregatePartitionCount { get; set; } = 16;

    /// <summary>
    /// Gets or sets an optional directory path for external aggregate temporary partition files.
    /// When null or empty, the process temp directory is used.
    /// </summary>
    public string? VolcanoExternalAggregateTempDirectory { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether external aggregate spill should adapt partition count based on observed volume.
    /// </summary>
    public bool VolcanoExternalAggregateAdaptivePartitioning { get; set; } = true;

    /// <summary>
    /// Gets or sets target rows per partition when adaptive aggregate partitioning is enabled.
    /// </summary>
    public int VolcanoExternalAggregateTargetRowsPerPartition { get; set; } = 4096;

    /// <summary>
    /// Gets or sets maximum partition count used by adaptive aggregate partitioning.
    /// </summary>
    public int VolcanoExternalAggregateMaxPartitionCount { get; set; } = 128;

    /// <summary>
    /// Gets or sets a value indicating whether Volcano join-cardinality feedback learning is enabled.
    /// </summary>
    public bool EnableVolcanoJoinCardinalityFeedback { get; set; } = true;

    /// <summary>
    /// Gets or sets a value indicating whether join-cardinality feedback should be persisted across engine restarts.
    /// </summary>
    public bool EnableVolcanoJoinCardinalityFeedbackPersistence { get; set; }

    /// <summary>
    /// Gets or sets optional file path used to persist join-cardinality feedback when persistence is enabled.
    /// </summary>
    public string? VolcanoJoinCardinalityFeedbackPersistenceFile { get; set; }

    /// <summary>
    /// Gets or sets the maximum number of learned join-cardinality entries retained in memory/persistence.
    /// </summary>
    public int VolcanoJoinCardinalityFeedbackMaxEntries { get; set; } = 4096;

    /// <summary>
    /// Gets or sets a value indicating whether vector distance predicates in WHERE are eligible for vector index prefilter fast path.
    /// </summary>
    public bool EnableVectorPredicateFastPath { get; set; } = true;

    /// <summary>
    /// Gets or sets the minimum base-table row count required before vector predicate fast path is considered.
    /// </summary>
    public int VectorPredicateFastPathMinRows { get; set; } = 128;

    /// <summary>
    /// Gets or sets the maximum candidate ratio allowed for vector predicate fast path.
    /// When computed top-K exceeds this fraction of the table, legacy filtering is preferred.
    /// </summary>
    public double VectorPredicateFastPathMaxTopKRatio { get; set; } = 0.6d;

    /// <summary>
    /// Gets or sets a multiplier used to expand estimated qualifying rows into ANN candidate top-K.
    /// </summary>
    public int VectorPredicateFastPathCandidateMultiplier { get; set; } = 3;

    /// <summary>
    /// Gets or sets an absolute cap for computed ANN candidate top-K in vector predicate fast path.
    /// </summary>
    public int VectorPredicateFastPathMaxTopK { get; set; } = 20000;

    /// <summary>
    /// Gets or sets a value indicating whether planner should log vector fast-path acceptance/rejection decisions.
    /// </summary>
    public bool EnableVectorPredicateFastPathTelemetry { get; set; } = true;

    /// <summary>
    /// Gets or sets maximum additional candidate expansion passes when LIMIT is present and post-filtered rows are insufficient.
    /// </summary>
    public int VectorPredicateFastPathMaxExpansionPasses { get; set; } = 2;

    /// <summary>
    /// Gets or sets the geometric growth factor for vector fast-path candidate expansion.
    /// </summary>
    public int VectorPredicateFastPathExpansionFactor { get; set; } = 2;

    /// <summary>
    /// Gets or sets a value indicating whether planner should track hybrid vector route counters.
    /// </summary>
    public bool EnableHybridRoutingTelemetryCounters { get; set; } = true;

    /// <summary>
    /// Gets or sets a value indicating whether planner should emit per-query hybrid routing telemetry.
    /// </summary>
    public bool EnableHybridRoutingPerQueryTelemetry { get; set; } = true;

    /// <summary>
    /// Gets or sets query interval for periodic aggregated hybrid telemetry snapshots.
    /// A value less than or equal to zero disables periodic snapshots.
    /// </summary>
    public int HybridRoutingTelemetrySnapshotIntervalQueries { get; set; } = 25;

    /// <summary>
    /// Gets or sets a value indicating whether hybrid ORDER BY path should use selectivity-informed initial top-K sizing.
    /// </summary>
    public bool EnableHybridOrderByAdaptiveInitialTopK { get; set; } = true;

    private readonly object _hybridRoutingTelemetrySync = new();
    private readonly Dictionary<string, int> _hybridRoutingTelemetryCounters = new(StringComparer.OrdinalIgnoreCase);
    private int _hybridRoutingProcessedQueries;
    private int _hybridRoutingUsedQueries;
    private long _hybridRoutingTotalExpansionPasses;
    private int _hybridRoutingLastSnapshotAtQueryCount;
    private int _hybridRoutingSnapshotEmissions;

    private bool? _walEnabled;

    /// <summary>
    /// The directory path where database files will be stored.
    /// Only required if StorageMode is Disk.
    /// </summary>
    public string? DiskStoragePath { get; set; }

    /// <summary>
    /// Gets or sets lock acquisition timeout in milliseconds.
    /// Use -1 for infinite wait.
    /// </summary>
    public int LockAcquireTimeoutMs { get; set; } = 30000;

    /// <summary>
    /// Gets or sets a value indicating whether write-ahead logging is enabled.
    /// </summary>
    public bool WalEnabled
    {
        get => _walEnabled ?? StorageMode == StorageMode.Disk;
        set => _walEnabled = value;
    }

    /// <summary>
    /// Gets or sets the WAL file name or path.
    /// </summary>
    public string WalFilePath { get; set; } = "datavo.wal";

    /// <summary>
    /// Gets or sets the number of WAL entries that can accumulate before checkpoint cleanup is considered.
    /// </summary>
    public int WalCheckpointThreshold { get; set; } = 1000;

    /// <summary>
    /// Gets or sets the file name or path used to persist MVCC transaction ID high-water mark.
    /// </summary>
    public string TransactionIdStateFilePath { get; set; } = "datavo.txid";

    /// <summary>
    /// Resolves the effective WAL file path for the current configuration.
    /// </summary>
    /// <returns>An absolute or base-directory-relative path to the WAL file.</returns>
    public string ResolveWalFilePath()
    {
        if (Path.IsPathRooted(WalFilePath))
        {
            return WalFilePath;
        }

        string baseDirectory = StorageMode == StorageMode.Disk
            ? (DiskStoragePath ?? "./datavo_data")
            : Directory.GetCurrentDirectory();

        return Path.Combine(baseDirectory, WalFilePath);
    }

    /// <summary>
    /// Resolves the effective transaction-ID state file path for the current configuration.
    /// </summary>
    /// <returns>An absolute or base-directory-relative path to the transaction ID state file.</returns>
    public string ResolveTransactionIdStateFilePath()
    {
        if (Path.IsPathRooted(TransactionIdStateFilePath))
        {
            return TransactionIdStateFilePath;
        }

        string baseDirectory = StorageMode == StorageMode.Disk
            ? (DiskStoragePath ?? "./datavo_data")
            : Directory.GetCurrentDirectory();

        return Path.Combine(baseDirectory, TransactionIdStateFilePath);
    }

    /// <summary>
    /// Increments a named hybrid routing counter bucket.
    /// </summary>
    /// <param name="bucket">Counter bucket key.</param>
    public void IncrementHybridRoutingCounter(string bucket)
    {
        if (!EnableHybridRoutingTelemetryCounters || string.IsNullOrWhiteSpace(bucket))
        {
            return;
        }

        lock (_hybridRoutingTelemetrySync)
        {
            _hybridRoutingTelemetryCounters.TryGetValue(bucket, out int current);
            _hybridRoutingTelemetryCounters[bucket] = current + 1;
        }
    }

    /// <summary>
    /// Gets the current value of a hybrid routing counter bucket.
    /// </summary>
    /// <param name="bucket">Counter bucket key.</param>
    /// <returns>The bucket value, or 0 when no samples were recorded.</returns>
    public int GetHybridRoutingCounter(string bucket)
    {
        if (string.IsNullOrWhiteSpace(bucket))
        {
            return 0;
        }

        lock (_hybridRoutingTelemetrySync)
        {
            return _hybridRoutingTelemetryCounters.TryGetValue(bucket, out int current)
                ? current
                : 0;
        }
    }

    /// <summary>
    /// Returns a point-in-time snapshot of hybrid routing counters.
    /// </summary>
    /// <returns>Read-only bucket/value pairs.</returns>
    public IReadOnlyDictionary<string, int> GetHybridRoutingCountersSnapshot()
    {
        lock (_hybridRoutingTelemetrySync)
        {
            return new Dictionary<string, int>(_hybridRoutingTelemetryCounters, StringComparer.OrdinalIgnoreCase);
        }
    }

    /// <summary>
    /// Clears all accumulated hybrid routing counters.
    /// </summary>
    public void ResetHybridRoutingCounters()
    {
        lock (_hybridRoutingTelemetrySync)
        {
            _hybridRoutingTelemetryCounters.Clear();
            _hybridRoutingProcessedQueries = 0;
            _hybridRoutingUsedQueries = 0;
            _hybridRoutingTotalExpansionPasses = 0;
            _hybridRoutingLastSnapshotAtQueryCount = 0;
            _hybridRoutingSnapshotEmissions = 0;
        }
    }

    /// <summary>
    /// Records a query-level hybrid routing telemetry sample and optionally emits a periodic aggregated snapshot.
    /// </summary>
    /// <param name="queryBuckets">Query-scoped bucket counts.</param>
    /// <param name="expansionPasses">Expansion pass count observed for the query.</param>
    /// <param name="snapshotMessage">When emitted, contains a preformatted aggregate snapshot message.</param>
    /// <returns><see langword="true"/> when a periodic snapshot should be emitted; otherwise <see langword="false"/>.</returns>
    public bool RecordHybridRoutingQueryAndTryBuildSnapshot(
        IReadOnlyDictionary<string, int> queryBuckets,
        int expansionPasses,
        out string? snapshotMessage)
    {
        snapshotMessage = null;

        if (!EnableHybridRoutingTelemetryCounters)
        {
            return false;
        }

        int safeExpansionPasses = Math.Max(0, expansionPasses);

        lock (_hybridRoutingTelemetrySync)
        {
            _hybridRoutingProcessedQueries++;
            _hybridRoutingTotalExpansionPasses += safeExpansionPasses;

            if (queryBuckets.Count > 0)
            {
                _hybridRoutingUsedQueries++;
            }

            foreach (var entry in queryBuckets)
            {
                if (string.IsNullOrWhiteSpace(entry.Key) || entry.Value <= 0)
                {
                    continue;
                }

                _hybridRoutingTelemetryCounters.TryGetValue(entry.Key, out int current);
                _hybridRoutingTelemetryCounters[entry.Key] = current + entry.Value;
            }

            int interval = HybridRoutingTelemetrySnapshotIntervalQueries;
            if (interval <= 0)
            {
                return false;
            }

            int deltaSinceSnapshot = _hybridRoutingProcessedQueries - _hybridRoutingLastSnapshotAtQueryCount;
            if (deltaSinceSnapshot < interval)
            {
                return false;
            }

            _hybridRoutingLastSnapshotAtQueryCount = _hybridRoutingProcessedQueries;
            _hybridRoutingSnapshotEmissions++;

            double avgExpansionPasses = _hybridRoutingProcessedQueries == 0
                ? 0d
                : (double)_hybridRoutingTotalExpansionPasses / _hybridRoutingProcessedQueries;

            string bucketSummary = _hybridRoutingTelemetryCounters.Count == 0
                ? "none"
                : string.Join(", ", _hybridRoutingTelemetryCounters
                    .OrderByDescending(pair => pair.Value)
                    .Take(8)
                    .Select(pair => $"{pair.Key}={pair.Value}"));

            snapshotMessage = $"queries={_hybridRoutingProcessedQueries}; used={_hybridRoutingUsedQueries}; avgExpansionPasses={avgExpansionPasses:F3}; buckets={bucketSummary}";
            return true;
        }
    }

    /// <summary>
    /// Gets the total number of SELECT queries recorded by hybrid routing telemetry.
    /// </summary>
    public int GetHybridRoutingProcessedQueryCount()
    {
        lock (_hybridRoutingTelemetrySync)
        {
            return _hybridRoutingProcessedQueries;
        }
    }

    /// <summary>
    /// Gets the number of queries that actually used a hybrid route bucket.
    /// </summary>
    public int GetHybridRoutingUsedQueryCount()
    {
        lock (_hybridRoutingTelemetrySync)
        {
            return _hybridRoutingUsedQueries;
        }
    }

    /// <summary>
    /// Gets the total expansion pass count accumulated across recorded queries.
    /// </summary>
    public long GetHybridRoutingTotalExpansionPasses()
    {
        lock (_hybridRoutingTelemetrySync)
        {
            return _hybridRoutingTotalExpansionPasses;
        }
    }

    /// <summary>
    /// Gets the number of periodic snapshot emissions.
    /// </summary>
    public int GetHybridRoutingSnapshotEmissions()
    {
        lock (_hybridRoutingTelemetrySync)
        {
            return _hybridRoutingSnapshotEmissions;
        }
    }
}
