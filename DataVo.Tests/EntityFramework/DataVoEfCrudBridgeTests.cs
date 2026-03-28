using DataVo.Data;
using DataVo.EntityFrameworkCore;
using DataVo.Tests.BrowserParity;
using Microsoft.EntityFrameworkCore;
using System.IO;

namespace DataVo.Tests.EntityFramework;

[BrowserTranslateIgnore("EntityFramework provider-bridge tests rely on EF runtime semantics and are validated in .NET lane.")]
public class DataVoEfCrudBridgeTests
{
    [Fact]
    public void SaveChangesToDataVo_CanInsertUpdateDeleteTrackedEntity()
    {
        string databaseName = $"datavo_ef_crud_{Guid.NewGuid():N}";
        string connectionString = $"StorageMode=Disk;DataSource={databaseName}";
        string databasePath = Path.Combine(Directory.GetCurrentDirectory(), databaseName);

        try
        {
            using (var context = CreateContext(connectionString))
            {
                var ticket = new Ticket
                {
                    Id = 1,
                    Title = "Initial",
                    IsClosed = false,
                    Score = 7.5,
                    DueDate = new DateTime(2026, 3, 13)
                };

                context.Add(ticket);
                int inserted = context.SaveChangesToDataVo();
                Assert.Equal(1, inserted);

                ticket.Title = "Updated";
                ticket.IsClosed = true;

                int updated = context.SaveChangesToDataVo();
                Assert.Equal(1, updated);

                context.Remove(ticket);
                int deleted = context.SaveChangesToDataVo();
                Assert.Equal(1, deleted);
            }

            using var connection = new DataVoConnection(connectionString);
            connection.Open();

            using var command = connection.CreateCommand();
            command.CommandText = "SELECT * FROM Tickets;";
            using var reader = command.ExecuteReader();

            Assert.False(reader.Read());
        }
        finally
        {
            if (Directory.Exists(databasePath))
            {
                Directory.Delete(databasePath, recursive: true);
            }
        }
    }

    [Fact]
    public void SaveChangesToDataVo_InsertsPrincipalBeforeDependent_WhenTrackedTogether()
    {
        string databaseName = $"datavo_ef_fk_{Guid.NewGuid():N}";
        string connectionString = $"StorageMode=Disk;DataSource={databaseName}";
        string databasePath = Path.Combine(Directory.GetCurrentDirectory(), databaseName);

        try
        {
            using (var context = CreateContext(connectionString))
            {
                var board = new Board { Id = 1, Name = "Roadmap" };
                var card = new Card { Id = 1, BoardId = 1, Title = "Phase 2" };

                context.Add(card);
                context.Add(board);

                int affected = context.SaveChangesToDataVo();
                Assert.Equal(2, affected);
            }

            using var connection = new DataVoConnection(connectionString);
            connection.Open();

            using var command = connection.CreateCommand();
            command.CommandText = "SELECT * FROM Cards WHERE BoardId = 1;";
            using var reader = command.ExecuteReader();

            Assert.True(reader.Read());
            Assert.Equal("Phase 2", reader["Title"]?.ToString());
        }
        finally
        {
            if (Directory.Exists(databasePath))
            {
                Directory.Delete(databasePath, recursive: true);
            }
        }
    }

    [Fact]
    public void SaveChanges_WithoutUseDataVoConfiguration_Throws()
    {
        using var context = CreateContext();
        context.Add(new Ticket { Id = 99, Title = "NoConfig", IsClosed = false, Score = 1.0, DueDate = new DateTime(2026, 3, 13) });

        var exception = Assert.Throws<InvalidOperationException>(() => context.SaveChangesToDataVo());
        Assert.Contains("UseDataVo", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Verifies that the <see cref="Infrastructure.Internal.DataVoSaveChangesInterceptor"/>
    /// fires automatically during <c>context.SaveChanges()</c> and writes inserted entities
    /// to the DataVo storage — no explicit call to <c>SaveChangesToDataVo()</c> needed.
    /// </summary>
    [Fact]
    public void SaveChanges_NativeEfApi_AutomaticallyWritesToDataVo()
    {
        string databaseName = $"datavo_ef_native_{Guid.NewGuid():N}";
        string connectionString = $"StorageMode=Disk;DataSource={databaseName}";
        string databasePath = Path.Combine(Directory.GetCurrentDirectory(), databaseName);

        try
        {
            using (var context = CreateContext(connectionString))
            {
                // Schema creation via the standard EF API; routes to DataVoDatabaseCreator.
                context.Database.EnsureCreated();

                var ticket = new Ticket
                {
                    Id = 1,
                    Title = "NativeEf",
                    IsClosed = false,
                    Score = 4.2,
                    DueDate = new DateTime(2026, 6, 1)
                };

                context.Add(ticket);

                // Standard EF SaveChanges — interceptor transparently writes to DataVo.
                int saved = context.SaveChanges();
                Assert.Equal(1, saved);
            }

            // Verify the row is persisted in DataVo independently of InMemory.
            using var connection = new DataVoConnection(connectionString);
            connection.Open();

            using var command = connection.CreateCommand();
            command.CommandText = "SELECT Title FROM Tickets WHERE Id = 1;";
            using var reader = command.ExecuteReader();

            Assert.True(reader.Read(), "Expected one row in DataVo Tickets.");
            Assert.Equal("NativeEf", reader["Title"]?.ToString());
        }
        finally
        {
            if (Directory.Exists(databasePath))
            {
                Directory.Delete(databasePath, recursive: true);
            }
        }
    }

    [Fact]
    public void SaveChanges_NativeEfApi_WithoutExplicitEnsureCreated_AutomaticallyCreatesSchemaAndWritesToDataVo()
    {
        string databaseName = $"datavo_ef_native_lazy_{Guid.NewGuid():N}";
        string connectionString = $"StorageMode=Disk;DataSource={databaseName}";
        string databasePath = Path.Combine(Directory.GetCurrentDirectory(), databaseName);

        try
        {
            using (var context = CreateContext(connectionString))
            {
                context.Add(new Ticket
                {
                    Id = 11,
                    Title = "LazySchema",
                    IsClosed = false,
                    Score = 8.4,
                    DueDate = new DateTime(2027, 1, 15)
                });

                int saved = context.SaveChanges();
                Assert.Equal(1, saved);
            }

            Assert.True(Directory.Exists(databasePath));

            using var connection = new DataVoConnection(connectionString);
            connection.Open();

            using var command = connection.CreateCommand();
            command.CommandText = "SELECT Title FROM Tickets WHERE Id = 11;";
            using var reader = command.ExecuteReader();

            Assert.True(reader.Read());
            Assert.Equal("LazySchema", reader["Title"]?.ToString());
        }
        finally
        {
            if (Directory.Exists(databasePath))
            {
                Directory.Delete(databasePath, recursive: true);
            }
        }
    }

    [Fact]
    public async Task SaveChangesAsync_NativeEfApi_WithoutExplicitEnsureCreated_AutomaticallyCreatesSchemaAndWritesToDataVo()
    {
        string databaseName = $"datavo_ef_native_lazy_async_{Guid.NewGuid():N}";
        string connectionString = $"StorageMode=Disk;DataSource={databaseName}";
        string databasePath = Path.Combine(Directory.GetCurrentDirectory(), databaseName);

        try
        {
            using (var context = CreateContext(connectionString))
            {
                context.Add(new Ticket
                {
                    Id = 12,
                    Title = "LazySchemaAsync",
                    IsClosed = true,
                    Score = 3.1,
                    DueDate = new DateTime(2027, 2, 20)
                });

                int saved = await context.SaveChangesAsync();
                Assert.Equal(1, saved);
            }

            Assert.True(Directory.Exists(databasePath));

            using var connection = new DataVoConnection(connectionString);
            connection.Open();

            using var command = connection.CreateCommand();
            command.CommandText = "SELECT Title FROM Tickets WHERE Id = 12;";
            using var reader = command.ExecuteReader();

            Assert.True(reader.Read());
            Assert.Equal("LazySchemaAsync", reader["Title"]?.ToString());
        }
        finally
        {
            if (Directory.Exists(databasePath))
            {
                Directory.Delete(databasePath, recursive: true);
            }
        }
    }

    /// <summary>
    /// Verifies the native typed fluent API — no raw connection string is needed.
    /// <c>UseDataVo(o => o.UseDiskStorage().WithDataSource(...))</c> should synthesise the
    /// connection string and write data to disk via the standard EF APIs.
    /// </summary>
    [Fact]
    public void SaveChanges_TypedFluentOptions_WritesToDataVo()
    {
        string databaseName = $"datavo_ef_typed_{Guid.NewGuid():N}";
        string databasePath = Path.Combine(Directory.GetCurrentDirectory(), databaseName);

        try
        {
            using (var context = CreateContextWithTypedOptions(DataVoStorageMode.Disk, databaseName))
            {
                context.Database.EnsureCreated();

                context.Add(new Ticket
                {
                    Id = 1,
                    Title = "TypedOptions",
                    IsClosed = true,
                    Score = 9.9,
                    DueDate = new DateTime(2026, 12, 31)
                });

                context.SaveChanges();
            }

            string connectionString = $"StorageMode=Disk;DataSource={databaseName}";
            using var connection = new DataVoConnection(connectionString);
            connection.Open();

            using var command = connection.CreateCommand();
            command.CommandText = "SELECT Score FROM Tickets WHERE Id = 1;";
            using var reader = command.ExecuteReader();

            Assert.True(reader.Read());
        }
        finally
        {
            if (Directory.Exists(databasePath))
            {
                Directory.Delete(databasePath, recursive: true);
            }
        }
    }

    private static BridgeContext CreateContextWithTypedOptions(DataVoStorageMode mode, string dataSource)
    {
        var optionsBuilder = new DbContextOptionsBuilder<BridgeContext>()
            .UseInMemoryDatabase($"ef_typed_{Guid.NewGuid():N}")
            .UseDataVo(o => o.UseStorageMode(mode).WithDataSource(dataSource));

        return new BridgeContext(optionsBuilder.Options);
    }

    private static BridgeContext CreateContext(string? connectionString = null)
    {
        var optionsBuilder = new DbContextOptionsBuilder<BridgeContext>()
            .UseInMemoryDatabase($"ef_crud_model_{Guid.NewGuid():N}");

        if (!string.IsNullOrWhiteSpace(connectionString))
        {
            optionsBuilder.UseDataVo(connectionString);
        }

        return new BridgeContext(
            optionsBuilder.Options);
    }

    private sealed class BridgeContext(DbContextOptions<BridgeContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Ticket>(entity =>
            {
                entity.ToTable("Tickets");
                entity.HasKey(static ticket => ticket.Id);
                entity.Property(static ticket => ticket.Title).HasMaxLength(120);
            });

            modelBuilder.Entity<Board>(entity =>
            {
                entity.ToTable("Boards");
                entity.HasKey(static board => board.Id);
                entity.Property(static board => board.Name).HasMaxLength(120);
            });

            modelBuilder.Entity<Card>(entity =>
            {
                entity.ToTable("Cards");
                entity.HasKey(static card => card.Id);
                entity.Property(static card => card.Title).HasMaxLength(120);
                entity.HasOne(static card => card.Board)
                    .WithMany(static board => board.Cards)
                    .HasForeignKey(static card => card.BoardId);
            });
        }
    }

    private sealed class Ticket
    {
        public int Id { get; set; }
        public string Title { get; set; } = string.Empty;
        public bool IsClosed { get; set; }
        public double Score { get; set; }
        public DateTime DueDate { get; set; }
    }

    private sealed class Board
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<Card> Cards { get; set; } = [];
    }

    private sealed class Card
    {
        public int Id { get; set; }
        public int BoardId { get; set; }
        public string Title { get; set; } = string.Empty;
        public Board? Board { get; set; }
    }
}
