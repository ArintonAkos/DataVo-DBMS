using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.StorageEngine;

public sealed class LsmStorageBackendConfigTests
{
    [Fact]
    public void StorageContext_LsmMode_ResolvesLsmBackend()
    {
        string root = Path.Combine(Path.GetTempPath(), $"datavo_lsm_backend_config_{Guid.NewGuid():N}");

        try
        {
            using var context = new StorageContext(new DataVoConfig
            {
                StorageMode = StorageMode.Lsm,
                DiskStoragePath = root
            });

            Assert.NotNull(context.Backend);
            Assert.Equal("Lsm", context.Backend!.BackendKind);
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    [Fact]
    public void StorageContext_DiskMode_StillResolvesDiskBackend()
    {
        string root = Path.Combine(Path.GetTempPath(), $"datavo_disk_backend_config_{Guid.NewGuid():N}");

        try
        {
            using var context = new StorageContext(new DataVoConfig
            {
                StorageMode = StorageMode.Disk,
                DiskStoragePath = root
            });

            Assert.NotNull(context.Backend);
            Assert.Equal("Disk", context.Backend!.BackendKind);
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }
}
