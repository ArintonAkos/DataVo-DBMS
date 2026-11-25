namespace DataVo.EntityFrameworkCore;

/// <summary>
/// Controls whether the DataVo database keeps its data in memory or persists it with the LSM storage engine.
/// </summary>
public enum DataVoStorageMode
{
    /// <summary>
    /// Data is persisted on disk by the LSM storage engine.
    /// This is the default when an explicit <c>StorageMode</c> is not set.
    /// </summary>
    Lsm = 0,

    /// <summary>
    /// Data is stored in memory only (non-persistent, ideal for unit tests).
    /// </summary>
    InMemory = 1
}
