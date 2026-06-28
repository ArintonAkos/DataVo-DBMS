namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>Controls whether LSM WAL appends force bytes through the OS durable flush path.</summary>
public enum LsmWalDurabilityMode
{
    /// <summary>Append each mutation and synchronously force it to stable storage before MemTable mutation.</summary>
    StrictFsync = 0,

    /// <summary>Append each mutation to the OS file cache without a synchronous durable flush.</summary>
    RelaxedOsBuffer = 1,
}
