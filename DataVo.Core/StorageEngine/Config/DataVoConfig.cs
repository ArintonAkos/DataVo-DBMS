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
