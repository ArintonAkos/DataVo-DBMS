using DataVo.EntityFrameworkCore;
using DataVo.Tests.BrowserParity;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace DataVo.Tests.EntityFramework;

[BrowserTranslateIgnore("EntityFramework provider-bridge tests rely on EF runtime semantics and are validated in .NET lane.")]
public class DataVoProviderBootstrapTests
{
    [Fact]
    public void UseDataVo_RegistersDataVoOptionsExtension()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        optionsBuilder.UseDataVo("StorageMode=InMemory;DataSource=EfBootstrap");

        var snapshot = optionsBuilder.Options.GetDataVoBootstrapOptions();
        Assert.NotNull(snapshot);
        Assert.Equal("StorageMode=InMemory;DataSource=EfBootstrap", snapshot!.ConnectionString);
    }

    [Fact]
    public void UseDataVo_AppliesProviderSpecificOptions()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        optionsBuilder.UseDataVo(
            "StorageMode=Lsm;DataSource=EfBootstrapDisk",
            dataVoOptions => dataVoOptions.EnableNativeQueryTranslationPreview());

        var snapshot = optionsBuilder.Options.GetDataVoBootstrapOptions();
        Assert.NotNull(snapshot);
        Assert.Equal("StorageMode=Lsm;DataSource=EfBootstrapDisk", snapshot!.ConnectionString);
        Assert.True(snapshot.NativeQueryTranslationPreviewEnabled);
        Assert.False(snapshot.IsBridgeOnlyMode);
    }

    [Fact]
    public void UseDataVo_WithTypedFluentOptions_BuildsEffectiveConnectionString()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        // Use the connection-string-free overload with typed fluent options.
        optionsBuilder.UseDataVo(o => o
            .UseInMemoryStorage()
            .WithDataSource("typed_test_db"));

        var snapshot = optionsBuilder.Options.GetDataVoBootstrapOptions();

        Assert.NotNull(snapshot);
        Assert.Null(snapshot!.ConnectionString);   // no raw connection string
        Assert.Equal(DataVoStorageMode.InMemory, snapshot.StorageMode);
        Assert.Equal("typed_test_db", snapshot.DataSource);
        Assert.Equal("StorageMode=InMemory;DataSource=typed_test_db", snapshot.EffectiveConnectionString);
        Assert.True(snapshot.IsBridgeOnlyMode);
    }

    [Fact]
    public void UseDataVo_LsmTypedOptions_BuildsEffectiveConnectionString()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        optionsBuilder.UseDataVo("DataSource=legacy",
            o => o.UseLsmStorage().WithDataSource("typed_lsm_db"));

        var snapshot = optionsBuilder.Options.GetDataVoBootstrapOptions();

        Assert.NotNull(snapshot);
        Assert.Equal(DataVoStorageMode.Lsm, snapshot!.StorageMode);
        Assert.Equal("typed_lsm_db", snapshot.DataSource);
        // Typed options take priority over raw connection string.
        Assert.Equal("StorageMode=Lsm;DataSource=typed_lsm_db", snapshot.EffectiveConnectionString);
    }

    [Fact]
    public void UseDataVo_DefaultProviderModeStatus_IsBridgeOnly()
    {
        var options = new DbContextOptionsBuilder<BootstrapContext>()
            .UseInMemoryDatabase($"ef_mode_default_{Guid.NewGuid():N}")
            .UseDataVo(o => o.UseInMemoryStorage().WithDataSource($"mode_default_{Guid.NewGuid():N}"))
            .Options;

        var mode = options.GetDataVoProviderModeStatus();

        Assert.NotNull(mode);
        Assert.Equal(DataVoProviderMode.BridgeOnly, mode!.Mode);
        Assert.True(mode.IsBridgeOnlyMode);
        Assert.False(mode.NativeQueryTranslationPreviewEnabled);
    }

    [Fact]
    public void UseDataVo_NativeTranslationPreview_ModeStatusReflectsTranslationPreview()
    {
        var options = new DbContextOptionsBuilder<BootstrapContext>()
            .UseInMemoryDatabase($"ef_mode_translation_{Guid.NewGuid():N}")
            .UseDataVo(o => o
                .UseInMemoryStorage()
                .WithDataSource($"mode_translation_{Guid.NewGuid():N}")
                .EnableNativeQueryTranslationPreview())
            .Options;

        var mode = options.GetDataVoProviderModeStatus();

        Assert.NotNull(mode);
        Assert.Equal(DataVoProviderMode.NativeTranslationPreview, mode!.Mode);
        Assert.False(mode.IsBridgeOnlyMode);
        Assert.True(mode.NativeQueryTranslationPreviewEnabled);
    }

    [Fact]
    public void UseDataVo_EnableNativeQueryTranslationPreview_IsCapturedInBootstrapAndPreviewStatus()
    {
        var options = new DbContextOptionsBuilder<BootstrapContext>()
            .UseInMemoryDatabase($"ef_translation_{Guid.NewGuid():N}")
            .UseDataVo(o => o
                .UseInMemoryStorage()
                .WithDataSource($"translation_{Guid.NewGuid():N}")
                .EnableNativeQueryTranslationPreview())
            .Options;

        var bootstrap = options.GetDataVoBootstrapOptions();
        var preview = options.GetDataVoProviderPreviewStatus();

        Assert.NotNull(bootstrap);
        Assert.NotNull(preview);
        Assert.True(bootstrap!.NativeQueryTranslationPreviewEnabled);
        Assert.False(bootstrap.IsBridgeOnlyMode);
        Assert.True(preview!.NativeQueryTranslationPreviewEnabled);
        Assert.False(preview.IsBridgeOnlyMode);
    }

    [Fact]
    public void AddEntityFrameworkDataVo_RegistersDatabaseCreatorAndModeResolver()
    {
        var services = new ServiceCollection();

        services.AddEntityFrameworkDataVo();
        Assert.Contains(services, descriptor =>
            string.Equals(descriptor.ImplementationType?.Name, "DataVoDatabaseCreator", StringComparison.Ordinal));
        Assert.Contains(services, descriptor =>
            string.Equals(descriptor.ServiceType.Name, "DataVoProviderModeResolver", StringComparison.Ordinal));
        Assert.DoesNotContain(services, descriptor =>
            string.Equals(descriptor.ServiceType.Name, "DataVoProviderBootstrapMarker", StringComparison.Ordinal));
    }

    [Fact]
    public void UseDataVo_WithoutHostProvider_ThrowsClearValidationError()
    {
        var options = new DbContextOptionsBuilder<BootstrapContext>()
            .UseDataVo("StorageMode=InMemory;DataSource=no_host")
            .Options;

        var ex = Assert.Throws<InvalidOperationException>(() =>
        {
            using var _ = new BootstrapContext(options);
        });

        Assert.Contains("host EF provider", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("UseInMemoryDatabase", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void UseDataVo_WithHostProvider_AllowsContextConstruction()
    {
        var options = new DbContextOptionsBuilder<BootstrapContext>()
            .UseInMemoryDatabase($"ef_host_{Guid.NewGuid():N}")
            .UseDataVo("StorageMode=InMemory;DataSource=has_host")
            .Options;

        using var context = new BootstrapContext(options);
        Assert.NotNull(context.Model);
    }

    [Fact]
    public void UseDataVo_RepeatedContextConstruction_DoesNotTriggerManyServiceProvidersWarning()
    {
        for (int i = 0; i < 30; i++)
        {
            var options = new DbContextOptionsBuilder<BootstrapContext>()
                .UseInMemoryDatabase("ef_host_shared_provider")
                .UseDataVo($"StorageMode=InMemory;DataSource=shared_host_{i}")
                .Options;

            using var context = new BootstrapContext(options);
            _ = context.Model;
        }
    }

    [Fact]
    public void DatabaseFacade_GetDataVoProviderModeStatus_ReturnsLiveMode()
    {
        var options = new DbContextOptionsBuilder<BootstrapContext>()
            .UseInMemoryDatabase($"ef_mode_live_{Guid.NewGuid():N}")
            .UseDataVo(o => o
                .UseInMemoryStorage()
                .WithDataSource($"mode_live_{Guid.NewGuid():N}")
                .EnableNativeQueryTranslationPreview())
            .Options;

        using var context = new BootstrapContext(options);

        var mode = context.Database.GetDataVoProviderModeStatus();

        Assert.NotNull(mode);
        Assert.Equal(DataVoProviderMode.NativeTranslationPreview, mode!.Mode);
        Assert.True(mode.NativeQueryTranslationPreviewEnabled);
    }

    [Fact]
    public void DatabaseFacade_GetDataVoProviderPreviewStatus_ReturnsRuntimePreviewFlags()
    {
        var options = new DbContextOptionsBuilder<BootstrapContext>()
            .UseInMemoryDatabase($"ef_runtime_preview_{Guid.NewGuid():N}")
            .UseDataVo(o => o
                .UseInMemoryStorage()
                .WithDataSource($"runtime_preview_{Guid.NewGuid():N}")
                .EnableNativeQueryTranslationPreview())
            .Options;

        using var context = new BootstrapContext(options);
        var preview = context.Database.GetDataVoProviderPreviewStatus();

        Assert.NotNull(preview);
        Assert.True(preview!.NativeQueryTranslationPreviewEnabled);
        Assert.False(preview.IsBridgeOnlyMode);
        Assert.Contains("StorageMode=InMemory", preview.EffectiveConnectionString, StringComparison.Ordinal);
    }

    private sealed class BootstrapContext(DbContextOptions<BootstrapContext> options) : DbContext(options)
    {
    }
}
