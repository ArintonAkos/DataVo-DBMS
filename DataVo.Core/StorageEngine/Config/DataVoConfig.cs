namespace DataVo.Core.StorageEngine.Config;

public enum StorageMode
{
    InMemory,
    Disk
}

public class DataVoConfig
{
    public StorageMode StorageMode { get; set; } = StorageMode.InMemory;
    
    /// <summary>
    /// The directory path where database files will be stored.
    /// Only required if StorageMode is Disk.
    /// </summary>
    public string? DiskStoragePath { get; set; }
}
