namespace DataVo.Core.StorageEngine.Config;

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

    private bool? _walEnabled;

    /// <summary>
    /// The directory path where database files will be stored.
    /// Only required if StorageMode is Disk.
    /// </summary>
    public string? DiskStoragePath { get; set; }

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
}
