#if NETSTANDARD2_1
namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>Rejects unverified durable publication on the experimental portable target.</summary>
internal static class LsmDurableFileOperations
{
    private const string UnsupportedMessage =
        "Durable LSM publication is not supported by the experimental netstandard2.1 target. " +
        "Use StorageMode.InMemory.";

    public static void WriteFileAtomically(
        string filePath,
        bool overwrite,
        Action<Stream> write,
        Action<LsmCrashPoint>? crashHook,
        LsmCrashPoint afterTempFileFsync,
        LsmCrashPoint afterRenameBeforeDirectoryFsync,
        LsmCrashPoint afterDirectoryFsync) =>
        throw CreateUnsupportedException();

    internal static void FsyncDirectory(string directoryPath) => throw CreateUnsupportedException();

    private static PlatformNotSupportedException CreateUnsupportedException() => new(UnsupportedMessage);
}
#endif
