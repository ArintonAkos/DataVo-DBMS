namespace DataVo.Core.StorageEngine.Config;

/// <summary>
/// Defines the physical storage modes supported by the engine.
/// </summary>
public enum StorageMode
{
    /// <summary>Stores data only in process memory.</summary>
    InMemory,

    /// <summary>Stores data in on-disk table files.</summary>
    Disk
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
