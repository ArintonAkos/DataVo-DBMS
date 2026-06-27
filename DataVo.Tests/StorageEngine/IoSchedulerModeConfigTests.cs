using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Backends;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.StorageEngine;

public sealed class IoSchedulerModeConfigTests
{
    [Fact]
    public void DataVoConfig_DefaultsIoSchedulerModeToOff()
    {
        var config = new DataVoConfig();

        Assert.Equal(IoSchedulerMode.Off, config.IoSchedulerMode);
    }

    [Fact]
    public void StorageContext_PassesIoSchedulerModeToDiskBackend()
    {
        string root = Path.Combine(Path.GetTempPath(), $"datavo_io_scheduler_config_{Guid.NewGuid():N}");

        try
        {
            var context = new StorageContext(new DataVoConfig
            {
                StorageMode = StorageMode.Disk,
                DiskStoragePath = root,
                IoSchedulerMode = IoSchedulerMode.PoolingOnly
            });

            var backend = Assert.IsType<DiskStorageBackend>(context.Backend);

            Assert.Equal(IoSchedulerMode.PoolingOnly, backend.IoSchedulerMode);
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
