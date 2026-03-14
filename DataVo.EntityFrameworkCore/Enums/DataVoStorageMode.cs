namespace DataVo.EntityFrameworkCore;

/// <summary>
/// Controls whether the DataVo database uses in-memory or disk-based storage.
/// </summary>
public enum DataVoStorageMode
{
    /// <summary>
    /// Data is stored on disk (persistent).
    /// This is the default when an explicit <c>StorageMode</c> is not set.
    /// </summary>
    Disk = 0,

    /// <summary>
    /// Data is stored in memory only (non-persistent, ideal for unit tests).
    /// </summary>
    InMemory = 1
}
