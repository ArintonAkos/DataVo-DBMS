using DataVo.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace DataVo.Tests.EntityFramework;

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
            "StorageMode=Disk;DataSource=EfBootstrapDisk",
            dataVoOptions => dataVoOptions.EnableBootstrapDiagnostics());

        var snapshot = optionsBuilder.Options.GetDataVoBootstrapOptions();
        Assert.NotNull(snapshot);
        Assert.True(snapshot!.BootstrapDiagnosticsEnabled);
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
    }

    [Fact]
    public void UseDataVo_DiskTypedOptions_BuildsEffectiveConnectionString()
    {
        var optionsBuilder = new DbContextOptionsBuilder();

        optionsBuilder.UseDataVo("DataSource=legacy",
            o => o.UseDiskStorage().WithDataSource("typed_disk_db"));

        var snapshot = optionsBuilder.Options.GetDataVoBootstrapOptions();

        Assert.NotNull(snapshot);
        Assert.Equal(DataVoStorageMode.Disk, snapshot!.StorageMode);
        Assert.Equal("typed_disk_db", snapshot.DataSource);
        // Typed options take priority over raw connection string.
        Assert.Equal("StorageMode=Disk;DataSource=typed_disk_db", snapshot.EffectiveConnectionString);
    }

    [Fact]
    public void AddEntityFrameworkDataVo_RegistersBootstrapMarker()
    {
        var services = new ServiceCollection();

        services.AddEntityFrameworkDataVo();
        Assert.Contains(services, descriptor =>
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

    private sealed class BootstrapContext(DbContextOptions<BootstrapContext> options) : DbContext(options)
    {
    }
}
