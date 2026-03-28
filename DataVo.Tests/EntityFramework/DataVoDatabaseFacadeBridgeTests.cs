using DataVo.EntityFrameworkCore;
using DataVo.Tests.BrowserParity;
using Microsoft.EntityFrameworkCore;
using System.IO;

namespace DataVo.Tests.EntityFramework;

[BrowserTranslateIgnore("EntityFramework provider-bridge tests rely on EF runtime semantics and are validated in .NET lane.")]
public class DataVoDatabaseFacadeBridgeTests
{
    [Fact]
    public void DatabaseFacade_EnsureDataVoCreatedAndDeleted_UsesUseDataVoConnection()
    {
        string databaseName = $"datavo_ef_dbfacade_{Guid.NewGuid():N}";
        string connectionString = $"StorageMode=Disk;DataSource={databaseName}";

        using (var context = CreateContext(connectionString))
        {
            bool created = context.Database.EnsureDataVoCreated();
            Assert.True(created);
            Assert.True(Directory.Exists(databaseName));

            bool deleted = context.Database.EnsureDataVoDeleted();
            Assert.True(deleted);
            Assert.False(Directory.Exists(databaseName));
        }
    }

    /// <summary>
    /// Verifies that the STANDARD EF Core APIs <c>Database.EnsureCreated()</c> and
    /// <c>Database.EnsureDeleted()</c> are routed to DataVo via the registered
    /// <see cref="Infrastructure.Internal.DataVoDatabaseCreator"/>.
    /// </summary>
    [Fact]
    public void DatabaseFacade_StandardEnsureCreated_RoutesToDataVo()
    {
        string databaseName = $"datavo_ef_creator_{Guid.NewGuid():N}";
        string connectionString = $"StorageMode=Disk;DataSource={databaseName}";

        using var context = CreateContext(connectionString);

        // Standard EF API — should now route to DataVoDatabaseCreator.
        bool created = context.Database.EnsureCreated();
        Assert.True(created);
        Assert.True(Directory.Exists(databaseName),
            "DataVo should have created the database directory via Database.EnsureCreated().");

        // Standard EF delete API.
        bool deleted = context.Database.EnsureDeleted();
        Assert.True(deleted);
        Assert.False(Directory.Exists(databaseName),
            "DataVo should have removed the database directory via Database.EnsureDeleted().");
    }

    private static FacadeContext CreateContext(string connectionString)
    {
        return new FacadeContext(
            new DbContextOptionsBuilder<FacadeContext>()
                .UseInMemoryDatabase($"ef_facade_model_{Guid.NewGuid():N}")
                .UseDataVo(connectionString)
                .Options);
    }

    private sealed class FacadeContext(DbContextOptions<FacadeContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Item>(entity =>
            {
                entity.ToTable("Items");
                entity.HasKey(static item => item.Id);
                entity.Property(static item => item.Name).HasMaxLength(120);
            });
        }
    }

    private sealed class Item
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
