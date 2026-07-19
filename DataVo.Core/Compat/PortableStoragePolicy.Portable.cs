#if NETSTANDARD2_1
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Core.Compat;

internal static class PortableStoragePolicy
{
    internal static void ThrowIfUnsupported(StorageMode mode)
    {
        if (mode is StorageMode.Disk or StorageMode.Lsm)
        {
            throw new PlatformNotSupportedException(
                $"StorageMode.{mode} is not supported on the experimental netstandard2.1 target. " +
                "StorageMode.InMemory is the only supported candidate scope.");
        }
    }
}
#endif
