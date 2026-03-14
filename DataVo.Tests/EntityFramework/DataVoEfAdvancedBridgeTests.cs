using DataVo.Data;
using DataVo.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using System.IO;

namespace DataVo.Tests.EntityFramework;

public class DataVoEfAdvancedBridgeTests
{
    [Fact]
    public void LoadFromDataVo_PullsExternalRowsIntoLinqQueryPipeline()
    {
        string db = $"datavo_ef_read_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var writer = CreateContext(cs))
            {
                writer.Database.EnsureCreated();
                writer.ExecuteSqlOnDataVo("INSERT INTO Boards (Id, Name) VALUES (1, 'Roadmap');");
                writer.ExecuteSqlOnDataVo("INSERT INTO Cards (Id, BoardId, Title) VALUES (10, 1, 'Phase A');");
            }

            using var reader = CreateContext(cs);

            // Without explicit load, the in-memory provider has no persisted rows yet.
            Assert.Empty(reader.Cards.ToList());

            reader.LoadFromDataVo();

            var cards = reader.Cards.Include(static c => c.Board).ToList();
            Assert.Single(cards);
            Assert.Equal("Phase A", cards[0].Title);
            Assert.NotNull(cards[0].Board);
            Assert.Equal("Roadmap", cards[0].Board!.Name);
        }
        finally
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
    }

    [Fact]
    public void EnsureCreatedAndLoad_ReturnsTrueThenFalse_ForDiskStorage()
    {
        string db = $"datavo_ef_idempotent_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using var ctx1 = CreateContext(cs);
            bool firstCreated = ctx1.EnsureCreatedAndLoad();
            Assert.True(firstCreated);

            using var ctx2 = CreateContext(cs);
            bool secondCreated = ctx2.EnsureCreatedAndLoad();
            Assert.False(secondCreated);
        }
        finally
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
    }

    [Fact]
    public void ExecuteDataVoSqlRaw_CanMutateAndReadBackData()
    {
        string db = $"datavo_ef_raw_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using var ctx = CreateContext(cs);
            ctx.Database.EnsureCreated();

            int inserted = ctx.Database.ExecuteDataVoSqlRaw("INSERT INTO Boards (Id, Name) VALUES (5, 'Ops');");
            Assert.Equal(1, inserted);

            ctx.Database.LoadFromDataVo();
            Assert.Single(ctx.Boards.Where(static b => b.Name == "Ops"));
        }
        finally
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
    }

    [Fact]
    public void ExecuteDataVoSqlRaw_DuplicateKey_ThrowsTypedDataVoEfException()
    {
        string db = $"datavo_ef_raw_err_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using var ctx = CreateContext(cs);
            ctx.Database.EnsureCreated();

            ctx.Database.ExecuteDataVoSqlRaw("INSERT INTO Boards (Id, Name) VALUES (1, 'First');");

            var ex = Assert.Throws<DataVoEfException>(() =>
                ctx.Database.ExecuteDataVoSqlRaw("INSERT INTO Boards (Id, Name) VALUES (1, 'Duplicate');"));

            Assert.Equal(DataVoEfOperation.RawSql, ex.Operation);
        }
        finally
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
    }

    [Fact]
    public void SaveChanges_DuplicatePrimaryKey_ThrowsTypedInsertException()
    {
        string db = $"datavo_ef_dup_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var ctx1 = CreateContext(cs))
            {
                ctx1.Database.EnsureCreated();
                ctx1.Boards.Add(new Board { Id = 1, Name = "A" });
                ctx1.SaveChanges();
            }

            using var ctx2 = CreateContext(cs);
            ctx2.Boards.Add(new Board { Id = 1, Name = "B" });

            var ex = Assert.Throws<DataVoEfException>(() => ctx2.SaveChanges());
            Assert.Equal(DataVoEfOperation.Insert, ex.Operation);
        }
        finally
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
    }

    [Fact]
    public void UseDataVo_TypedConfigMissingDataSource_ThrowsOnUse()
    {
        var options = new DbContextOptionsBuilder<AdvancedContext>()
            .UseInMemoryDatabase($"ef_missing_ds_{Guid.NewGuid():N}")
            .UseDataVo(o => o.UseDiskStorage())
            .Options;

        var ex = Assert.Throws<InvalidOperationException>(() =>
        {
            using var _ = new AdvancedContext(options);
        });
        Assert.Contains("WithDataSource", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void LoadFromDataVo_WithoutUseDataVo_ThrowsInvalidOperation()
    {
        var options = new DbContextOptionsBuilder<AdvancedContext>()
            .UseInMemoryDatabase($"ef_no_cfg_{Guid.NewGuid():N}")
            .Options;

        using var context = new AdvancedContext(options);

        var ex = Assert.Throws<InvalidOperationException>(() => context.LoadFromDataVo());
        Assert.Contains("UseDataVo", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void DataVoCanConnect_ReturnsTrueWhenConfigured_AndFalseWhenNotConfigured()
    {
        string db = $"datavo_ef_connect_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var configured = CreateContext(cs))
            {
                Assert.True(configured.Database.DataVoCanConnect());
            }

            var noCfgOptions = new DbContextOptionsBuilder<AdvancedContext>()
                .UseInMemoryDatabase($"ef_nocfg_{Guid.NewGuid():N}")
                .Options;
            using var noCfg = new AdvancedContext(noCfgOptions);

            Assert.False(noCfg.Database.DataVoCanConnect());
        }
        finally
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
    }

    [Fact]
    public void ExecuteSqlOnDataVo_ParameterizedBoolAndString_Works()
    {
        string db = $"datavo_ef_param_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using var ctx = CreateContext(cs);
            ctx.Database.EnsureCreated();

            int inserted = ctx.ExecuteSqlOnDataVo(
                "INSERT INTO Boards (Id, Name) VALUES (@id, @name);",
                ("@id", 99),
                ("@name", "Obrien"));
            Assert.Equal(1, inserted);

            // Validate bool parameter path now emits true/false (not 1/0) for BIT.
            int insertedRow = ctx.ExecuteSqlOnDataVo(
                "INSERT INTO AdvancedRows (Id, Name, Score, Level, IsActive) VALUES (@id, @name, @score, @level, @active);",
                ("@id", Guid.NewGuid()),
                ("@name", "ParamBool"),
                ("@score", 1.5),
                ("@level", 1),
                ("@active", true));
            Assert.Equal(1, insertedRow);

            ctx.LoadFromDataVo();

            Assert.Single(ctx.Boards.Where(static b => b.Id == 99 && b.Name == "Obrien"));
            Assert.Single(ctx.AdvancedRows.Where(static x => x.Name == "ParamBool" && x.IsActive));
        }
        finally
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
    }

    [Fact]
    public void LoadFromDataVo_CalledTwice_DoesNotDuplicateTrackedRows()
    {
        string db = $"datavo_ef_reload_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var writer = CreateContext(cs))
            {
                writer.Database.EnsureCreated();
                writer.ExecuteSqlOnDataVo("INSERT INTO Boards (Id, Name) VALUES (1, 'Once');");
            }

            using var reader = CreateContext(cs);

            reader.LoadFromDataVo();
            int firstCount = reader.Boards.Count();

            reader.LoadFromDataVo();
            int secondCount = reader.Boards.Count();

            Assert.Equal(1, firstCount);
            Assert.Equal(1, secondCount);
        }
        finally
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
    }

    [Fact]
    public void EnsureDeleted_InMemoryStorage_ReturnsTrue()
    {
        var options = new DbContextOptionsBuilder<AdvancedContext>()
            .UseInMemoryDatabase($"ef_mem_delete_{Guid.NewGuid():N}")
            .UseDataVo(o => o.UseInMemoryStorage().WithDataSource($"mem_{Guid.NewGuid():N}"))
            .Options;

        using var context = new AdvancedContext(options);

        bool deleted = context.Database.EnsureDeleted();
        Assert.True(deleted);
    }

    [Fact]
    public void LoadFromDataVo_ConvertsNullableAndGuidAndEnumValues()
    {
        string db = $"datavo_ef_conv_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);
        Guid id = Guid.NewGuid();

        try
        {
            using (var ctx = CreateContext(cs))
            {
                ctx.Database.EnsureCreated();
                ctx.ExecuteSqlOnDataVo($"INSERT INTO AdvancedRows (Id, Name, Score, Level, IsActive) VALUES ('{id}', 'Alpha', NULL, 2, true);");
            }

            using var reader = CreateContext(cs);
            reader.LoadFromDataVo();

            var row = Assert.Single(reader.AdvancedRows.ToList());
            Assert.Equal(id, row.Id);
            Assert.Equal("Alpha", row.Name);
            Assert.Null(row.Score);
            Assert.Equal(Priority.Medium, row.Level);
            Assert.True(row.IsActive);
        }
        finally
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
    }

    [Fact]
    public async Task SaveChangesAsync_PersistsToDataVoAndCanBeReadByAnotherContext()
    {
        string db = $"datavo_ef_async_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var writer = CreateContext(cs))
            {
                writer.Boards.Add(new Board { Id = 7, Name = "Async" });
                int affected = await writer.SaveChangesAsync();
                Assert.Equal(1, affected);
            }

            using var reader = CreateContext(cs);
            reader.LoadFromDataVo();

            Assert.Single(reader.Boards.Where(static b => b.Id == 7));
        }
        finally
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
    }

    private static AdvancedContext CreateContext(string connectionString)
    {
        var options = new DbContextOptionsBuilder<AdvancedContext>()
            .UseInMemoryDatabase($"ef_adv_{Guid.NewGuid():N}")
            .UseDataVo(connectionString)
            .Options;

        return new AdvancedContext(options);
    }

    private sealed class AdvancedContext(DbContextOptions<AdvancedContext> options) : DataVoDbContext(options)
    {
        public DbSet<Board> Boards => Set<Board>();
        public DbSet<Card> Cards => Set<Card>();
        public DbSet<AdvancedRow> AdvancedRows => Set<AdvancedRow>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Board>(entity =>
            {
                entity.ToTable("Boards");
                entity.HasKey(static x => x.Id);
                entity.Property(static x => x.Name).HasMaxLength(120);
            });

            modelBuilder.Entity<Card>(entity =>
            {
                entity.ToTable("Cards");
                entity.HasKey(static x => x.Id);
                entity.Property(static x => x.Title).HasMaxLength(120);
                entity.HasOne(static x => x.Board)
                    .WithMany(static b => b.Cards)
                    .HasForeignKey(static x => x.BoardId);
            });

            modelBuilder.Entity<AdvancedRow>(entity =>
            {
                entity.ToTable("AdvancedRows");
                entity.HasKey(static x => x.Id);
                entity.Property(static x => x.Id);
                entity.Property(static x => x.Name).HasMaxLength(80);
                entity.Property(static x => x.Score);
                entity.Property(static x => x.Level);
                entity.Property(static x => x.IsActive);
            });
        }
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

    private sealed class AdvancedRow
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public double? Score { get; set; }
        public Priority Level { get; set; }
        public bool IsActive { get; set; }
    }

    private enum Priority
    {
        Low = 1,
        Medium = 2,
        High = 3
    }
}
