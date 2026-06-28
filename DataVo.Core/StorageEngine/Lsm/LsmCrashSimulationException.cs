namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>Exception used by tests to stop an LSM durability sequence at a precise crash point.</summary>
internal sealed class LsmCrashSimulationException : Exception
{
    public LsmCrashSimulationException(LsmCrashPoint crashPoint)
        : base($"Simulated LSM crash at {crashPoint}.")
    {
        CrashPoint = crashPoint;
    }

    public LsmCrashPoint CrashPoint { get; }
}

/// <summary>Durability checkpoints where tests can simulate a process crash.</summary>
internal enum LsmCrashPoint
{
    AfterSstableTempFileFsyncBeforeRename,
    AfterSstableRenameBeforeDirectoryFsync,
    AfterSstableDirectoryFsyncBeforeManifest,
    AfterManifestTempFileFsyncBeforeRename,
    AfterManifestRenameBeforeDirectoryFsync,
    AfterManifestDirectoryFsyncBeforeWalClear,
}
