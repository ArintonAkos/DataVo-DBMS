#if NET10_0_OR_GREATER
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Core.Compat;

internal static class PortableStoragePolicy
{
    internal static void ThrowIfUnsupported(StorageMode mode)
    {
    }
}
#endif
