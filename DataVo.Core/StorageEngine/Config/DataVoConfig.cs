namespace DataVo.Core.StorageEngine.Config;

public enum StorageMode
{
    InMemory,
    Disk
}

public class DataVoConfig
{
    public StorageMode StorageMode { get; set; } = StorageMode.InMemory;

    private bool? _walEnabled;

    /// <summary>
    /// The directory path where database files will be stored.
    /// Only required if StorageMode is Disk.
    /// </summary>
    public string? DiskStoragePath { get; set; }

    public bool WalEnabled
    {
        get => _walEnabled ?? StorageMode == StorageMode.Disk;
        set => _walEnabled = value;
    }

    public string WalFilePath { get; set; } = "datavo.wal";

    public int WalCheckpointThreshold { get; set; } = 1000;

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
