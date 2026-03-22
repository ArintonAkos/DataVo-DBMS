using DataVo.Data;
using DataVo.EntityFrameworkCore;
using DataVo.EntityFrameworkCore.Infrastructure;
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

    [Fact]
    public void QueryFromDataVo_GuardedSyncQuery_RefreshesAndAppliesShape()
    {
        string db = $"datavo_ef_guarded_sync_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var writer = CreateContext(cs))
            {
                writer.Database.EnsureCreated();
                writer.ExecuteSqlOnDataVo("INSERT INTO Boards (Id, Name) VALUES (1, 'Roadmap');");
                writer.ExecuteSqlOnDataVo("INSERT INTO Cards (Id, BoardId, Title) VALUES (20, 1, 'Guarded');");
            }

            using var reader = CreateContext(cs);

            var cards = reader.QueryFromDataVo<Card>(q =>
                q.Include(static c => c.Board)
                 .Where(static c => c.Title == "Guarded"));

            var card = Assert.Single(cards);
            Assert.Equal(20, card.Id);
            Assert.NotNull(card.Board);
            Assert.Equal("Roadmap", card.Board!.Name);
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
    public async Task QueryFromDataVoAsync_GuardedAsyncQuery_RefreshesAndAppliesShape()
    {
        string db = $"datavo_ef_guarded_async_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var writer = CreateContext(cs))
            {
                writer.Database.EnsureCreated();
                writer.ExecuteSqlOnDataVo("INSERT INTO Boards (Id, Name) VALUES (2, 'Platform');");
                writer.ExecuteSqlOnDataVo("INSERT INTO Cards (Id, BoardId, Title) VALUES (21, 2, 'GuardedAsync');");
            }

            using var reader = CreateContext(cs);

            var cards = await reader.QueryFromDataVoAsync<Card>(q =>
                q.Where(static c => c.BoardId == 2));

            var card = Assert.Single(cards);
            Assert.Equal("GuardedAsync", card.Title);
        }
        finally
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
    }

    // ------------------------------------------------------------------ User-style LINQ tests (plain DbSet queries)

    [Fact]
    public void UserStyleLinq_AfterLoadFromDataVo_WhereOrderByTake_Works()
    {
        string db = $"datavo_userstyle_linq_basic_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var seed = CreateContext(cs))
            {
                seed.Database.EnsureCreated();
                seed.ExecuteSqlOnDataVo("INSERT INTO AdvancedRows (Id, Name, Score, Level, IsActive) VALUES (@id, @name, @score, @level, @active);",
                    ("@id", Guid.NewGuid()), ("@name", "Alpha"), ("@score", 9.0), ("@level", 3), ("@active", true));
                seed.ExecuteSqlOnDataVo("INSERT INTO AdvancedRows (Id, Name, Score, Level, IsActive) VALUES (@id, @name, @score, @level, @active);",
                    ("@id", Guid.NewGuid()), ("@name", "alpha"), ("@score", 6.0), ("@level", 2), ("@active", true));
                seed.ExecuteSqlOnDataVo("INSERT INTO AdvancedRows (Id, Name, Score, Level, IsActive) VALUES (@id, @name, @score, @level, @active);",
                    ("@id", Guid.NewGuid()), ("@name", "Beta"), ("@score", 8.0), ("@level", 1), ("@active", false));
            }

            using var ctx = CreateContext(cs);
            ctx.LoadFromDataVo();

            var rows = ctx.AdvancedRows
                .Where(row => row.Name.ToLower() == "alpha")
                .OrderByDescending(row => row.Score)
                .Take(1)
                .ToList();

            var row = Assert.Single(rows);
            Assert.Equal("Alpha", row.Name);
            Assert.Equal(9.0, row.Score);
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
    public void UserStyleLinq_AfterLoadFromDataVo_IncludeNavigation_Works()
    {
        string db = $"datavo_userstyle_linq_include_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var seed = CreateContext(cs))
            {
                seed.Database.EnsureCreated();
                seed.ExecuteSqlOnDataVo("INSERT INTO Boards (Id, Name) VALUES (1, 'Roadmap');");
                seed.ExecuteSqlOnDataVo("INSERT INTO Cards (Id, BoardId, Title) VALUES (100, 1, 'Milestone');");
            }

            using var ctx = CreateContext(cs);
            ctx.LoadFromDataVo();

            var cards = ctx.Cards
                .Include(card => card.Board)
                .Where(card => card.Title.Contains("stone"))
                .ToList();

            var card = Assert.Single(cards);
            Assert.Equal("Milestone", card.Title);
            Assert.NotNull(card.Board);
            Assert.Equal("Roadmap", card.Board!.Name);
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
    public void UserStyleLinq_AfterLoadFromDataVo_ProjectionSelect_Works()
    {
        string db = $"datavo_userstyle_linq_projection_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var seed = CreateContext(cs))
            {
                seed.Database.EnsureCreated();
                seed.ExecuteSqlOnDataVo("INSERT INTO AdvancedRows (Id, Name, Score, Level, IsActive) VALUES (@id, @name, @score, @level, @active);",
                    ("@id", Guid.NewGuid()), ("@name", "A1"), ("@score", 5.5), ("@level", 2), ("@active", true));
                seed.ExecuteSqlOnDataVo("INSERT INTO AdvancedRows (Id, Name, Score, Level, IsActive) VALUES (@id, @name, @score, @level, @active);",
                    ("@id", Guid.NewGuid()), ("@name", "A2"), ("@score", null), ("@level", 1), ("@active", true));
            }

            using var ctx = CreateContext(cs);
            ctx.LoadFromDataVo();

            var dtos = ctx.AdvancedRows
                .Where(row => row.Name.StartsWith("A"))
                .OrderBy(row => row.Name)
                .Select(row => new AdvancedRowDto(row.Name, (row.Score ?? 0) + 1))
                .ToList();

            Assert.Equal(2, dtos.Count);
            Assert.Equal("A1", dtos[0].Name);
            Assert.Equal(6.5, dtos[0].Value);
            Assert.Equal("A2", dtos[1].Name);
            Assert.Equal(1.0, dtos[1].Value);
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
    public void UserStyleLinq_DiskContext_FloatRoundtrip_Works()
    {
        string db = $"datavo_userstyle_linq_float_disk_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var seed = CreateContext(cs))
            {
                seed.Database.EnsureCreated();
                seed.ExecuteSqlOnDataVo("INSERT INTO AdvancedRows (Id, Name, Score, Level, IsActive) VALUES (@id, @name, @score, @level, @active);",
                    ("@id", Guid.NewGuid()), ("@name", "F1"), ("@score", 1.25), ("@level", 2), ("@active", true));
                seed.ExecuteSqlOnDataVo("INSERT INTO AdvancedRows (Id, Name, Score, Level, IsActive) VALUES (@id, @name, @score, @level, @active);",
                    ("@id", Guid.NewGuid()), ("@name", "F2"), ("@score", -3.5), ("@level", 1), ("@active", true));
                seed.ExecuteSqlOnDataVo("INSERT INTO AdvancedRows (Id, Name, Score, Level, IsActive) VALUES (@id, @name, @score, @level, @active);",
                    ("@id", Guid.NewGuid()), ("@name", "F3"), ("@score", null), ("@level", 3), ("@active", false));
            }

            using var ctx = CreateContext(cs);
            ctx.LoadFromDataVo();

            var rows = ctx.AdvancedRows
                .OrderBy(row => row.Name)
                .ToList();

            Assert.Equal(3, rows.Count);
            Assert.NotNull(rows[0].Score);
            Assert.NotNull(rows[1].Score);
            Assert.Null(rows[2].Score);
            Assert.InRange(rows[0].Score!.Value, 1.2499, 1.2501);
            Assert.InRange(rows[1].Score!.Value, -3.5001, -3.4999);

            var positive = ctx.AdvancedRows
                .Where(row => (row.Score ?? 0) > 0)
                .Select(row => row.Name)
                .ToList();

            Assert.Single(positive);
            Assert.Equal("F1", positive[0]);
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
    public void QueryFromDataVo_WhenQueryShapeThrows_WrapsAsTypedQueryOperation()
    {
        string db = $"datavo_ef_guarded_err_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using var context = CreateContext(cs);
            context.Database.EnsureCreated();

            var ex = Assert.Throws<DataVoEfException>(() =>
                context.QueryFromDataVo<Board>(_ => throw new FormatException("boom")));

            Assert.Equal(DataVoEfOperation.Query, ex.Operation);
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
    public void QueryFromDataVo_GroupByShape_IsBlockedWithTypedQueryOperation()
    {
        string db = $"datavo_ef_guarded_block_sync_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var writer = CreateContext(cs))
            {
                writer.Database.EnsureCreated();
                writer.ExecuteSqlOnDataVo("INSERT INTO Boards (Id, Name) VALUES (1, 'One');");
            }

            using var reader = CreateContext(cs);

            var ex = Assert.Throws<DataVoEfException>(() =>
                reader.QueryFromDataVo<Board>(q =>
                    q.GroupBy(static b => b.Name)
                     .Select(static g => g.First())));

            Assert.Equal(DataVoEfOperation.Query, ex.Operation);
            Assert.Contains("GroupBy", ex.Message, StringComparison.OrdinalIgnoreCase);
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
    public async Task QueryFromDataVoAsync_GroupByShape_IsBlockedWithTypedQueryOperation()
    {
        string db = $"datavo_ef_guarded_block_async_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var writer = CreateContext(cs))
            {
                writer.Database.EnsureCreated();
                writer.ExecuteSqlOnDataVo("INSERT INTO Boards (Id, Name) VALUES (1, 'One');");
            }

            using var reader = CreateContext(cs);

            var ex = await Assert.ThrowsAsync<DataVoEfException>(async () =>
                await reader.QueryFromDataVoAsync<Board>(q =>
                    q.GroupBy(static b => b.Name)
                     .Select(static g => g.First())));

            Assert.Equal(DataVoEfOperation.Query, ex.Operation);
            Assert.Contains("GroupBy", ex.Message, StringComparison.OrdinalIgnoreCase);
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
    public void GuardedQueryCapabilities_ExposeSupportedAndBlockedOperators()
    {
        var capabilities = DataVoDbContext.GetGuardedQueryCapabilities();

        Assert.Contains("Where", capabilities.SupportedOperators);
        Assert.Contains("Count", capabilities.SupportedOperators);
        Assert.Contains("GroupBy", capabilities.BlockedOperators);
        Assert.Contains("Join", capabilities.BlockedOperators);
        Assert.False(string.IsNullOrWhiteSpace(capabilities.Guidance));
    }

    [Fact]
    public void CanExecuteGuardedQuery_ReturnsTrueForSupportedShape_AndFalseForBlockedShape()
    {
        var options = new DbContextOptionsBuilder<AdvancedContext>()
            .UseInMemoryDatabase($"ef_can_guard_{Guid.NewGuid():N}")
            .UseDataVo(o => o.UseInMemoryStorage().WithDataSource($"mem_{Guid.NewGuid():N}"))
            .Options;

        using var context = new AdvancedContext(options);

        bool supported = context.CanExecuteGuardedQuery<Board>(
            q => q.Where(static b => b.Id > 0),
            out string? supportedReason);
        Assert.True(supported);
        Assert.Null(supportedReason);

        bool blocked = context.CanExecuteGuardedQuery<Board>(
            q => q.GroupBy(static b => b.Name).Select(static g => g.First()),
            out string? blockedReason);
        Assert.False(blocked);
        Assert.Contains("GroupBy", blockedReason, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void AnyFromDataVo_CountFromDataVo_FirstOrDefaultFromDataVo_WorkForMatchAndNoMatch()
    {
        string db = $"datavo_ef_guarded_aggr_sync_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var writer = CreateContext(cs))
            {
                writer.Database.EnsureCreated();
                writer.ExecuteSqlOnDataVo("INSERT INTO Boards (Id, Name) VALUES (1, 'A');");
                writer.ExecuteSqlOnDataVo("INSERT INTO Boards (Id, Name) VALUES (2, 'B');");
            }

            using var reader = CreateContext(cs);

            Assert.True(reader.AnyFromDataVo<Board>(static b => b.Id == 1));
            Assert.False(reader.AnyFromDataVo<Board>(static b => b.Id == 999));

            Assert.Equal(2, reader.CountFromDataVo<Board>());
            Assert.Equal(1, reader.CountFromDataVo<Board>(static b => b.Name == "A"));

            var match = reader.FirstOrDefaultFromDataVo<Board>(static b => b.Id == 2);
            Assert.NotNull(match);
            Assert.Equal("B", match!.Name);

            var noMatch = reader.FirstOrDefaultFromDataVo<Board>(static b => b.Id == 999);
            Assert.Null(noMatch);
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
    public async Task AnyCountFirstOrDefaultFromDataVoAsync_WorkForMatchAndNoMatch()
    {
        string db = $"datavo_ef_guarded_aggr_async_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var writer = CreateContext(cs))
            {
                writer.Database.EnsureCreated();
                writer.ExecuteSqlOnDataVo("INSERT INTO Boards (Id, Name) VALUES (10, 'AsyncA');");
                writer.ExecuteSqlOnDataVo("INSERT INTO Boards (Id, Name) VALUES (11, 'AsyncB');");
            }

            using var reader = CreateContext(cs);

            Assert.True(await reader.AnyFromDataVoAsync<Board>(static b => b.Id == 10));
            Assert.False(await reader.AnyFromDataVoAsync<Board>(static b => b.Id == 999));

            Assert.Equal(2, await reader.CountFromDataVoAsync<Board>());
            Assert.Equal(1, await reader.CountFromDataVoAsync<Board>(static b => b.Name == "AsyncB"));

            var match = await reader.FirstOrDefaultFromDataVoAsync<Board>(static b => b.Id == 11);
            Assert.NotNull(match);
            Assert.Equal("AsyncB", match!.Name);

            var noMatch = await reader.FirstOrDefaultFromDataVoAsync<Board>(static b => b.Id == 999);
            Assert.Null(noMatch);
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
    public void AggregateHelpers_BlockUnsupportedShapeWithTypedQueryOperation()
    {
        string db = $"datavo_ef_guarded_aggr_block_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var writer = CreateContext(cs))
            {
                writer.Database.EnsureCreated();
                writer.ExecuteSqlOnDataVo("INSERT INTO Boards (Id, Name) VALUES (1, 'One');");
            }

            using var reader = CreateContext(cs);

            var ex = Assert.Throws<DataVoEfException>(() =>
                reader.QueryFromDataVo<Board>(q =>
                    q.Union(q)));

            Assert.Equal(DataVoEfOperation.Query, ex.Operation);
            Assert.Contains("Union", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
    }

    // ------------------------------------------------------------------ DatabaseFacade capability tests

    [Fact]
    public void DatabaseFacade_GetDataVoGuardedQueryCapabilities_ReturnsExpectedProfile()
    {
        string cs = $"StorageMode=InMemory;DataSource=cap_facade_{Guid.NewGuid():N}";
        using var ctx = CreateContext(cs);

        var caps = ctx.Database.GetDataVoGuardedQueryCapabilities();

        Assert.NotNull(caps);
        Assert.NotEmpty(caps.SupportedOperators);
        Assert.NotEmpty(caps.BlockedOperators);
        Assert.Contains("GroupBy", caps.BlockedOperators);
        Assert.Contains("Join", caps.BlockedOperators);
        Assert.Contains("Where", caps.SupportedOperators);
        Assert.Contains("OrderBy", caps.SupportedOperators);
        Assert.NotEmpty(caps.Guidance);
    }

    [Fact]
    public void DatabaseFacade_CanExecuteGuardedQuery_PassesForSimpleWhere()
    {
        string cs = $"StorageMode=InMemory;DataSource=cap_where_{Guid.NewGuid():N}";
        using var ctx = CreateContext(cs);

        bool ok = ctx.Database.CanExecuteGuardedQuery<AdvancedRow>(
            q => q.Where(r => r.IsActive),
            out string? reason);

        Assert.True(ok);
        Assert.Null(reason);
    }

    [Fact]
    public void DatabaseFacade_CanExecuteGuardedQuery_FailsForJoin()
    {
        string cs = $"StorageMode=InMemory;DataSource=cap_join_{Guid.NewGuid():N}";
        using var ctx = CreateContext(cs);

        bool ok = ctx.Database.CanExecuteGuardedQuery<AdvancedRow>(
            q => q.Join(
                    q,
                    outer => outer.Id,
                    inner => inner.Id,
                    (o, i) => o),
            out string? reason);

        Assert.False(ok);
        Assert.NotNull(reason);
        Assert.Contains("Join", reason);
    }

    [Fact]
    public void DatabaseFacade_CanExecuteGuardedQuery_ThrowsForNonDataVoContext()
    {
        var options = new DbContextOptionsBuilder<PlainContext>()
            .UseInMemoryDatabase($"plain_{Guid.NewGuid():N}")
            .UseDataVo($"StorageMode=InMemory;DataSource=plain_{Guid.NewGuid():N}")
            .Options;

        using var ctx = new PlainContext(options);

        Assert.Throws<InvalidOperationException>(() =>
            ctx.Database.CanExecuteGuardedQuery<PlainRow>(
                q => q.Where(r => r.Id > 0),
                out _));
    }

    // ------------------------------------------------------------------ ProjectFromDataVo tests

    [Fact]
    public void ProjectFromDataVo_ReturnsProjectedDtos()
    {
        string db = $"datavo_proj_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var seed = CreateContext(cs))
            {
                seed.Database.EnsureCreated();
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "Alpha", Score = 9.5, IsActive = true });
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "Beta", Score = 4.0, IsActive = false });
                seed.SaveChanges();
            }

            using var ctx = CreateContext(cs);
            var dtos = ctx.ProjectFromDataVo<AdvancedRow, AdvancedRowDto>(
                selector: r => new AdvancedRowDto(r.Name, r.Score ?? 0));

            Assert.Equal(2, dtos.Count);
            Assert.Contains(dtos, d => d.Name == "Alpha" && d.Value == 9.5);
            Assert.Contains(dtos, d => d.Name == "Beta" && d.Value == 4.0);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task ProjectFromDataVoAsync_ReturnsProjectedDtos()
    {
        string db = $"datavo_proj_async_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var seed = CreateContext(cs))
            {
                seed.Database.EnsureCreated();
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "Gamma", Score = 7.0, IsActive = true });
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "Delta", Score = 2.5, IsActive = true });
                await seed.SaveChangesAsync();
            }

            using var ctx = CreateContext(cs);
            var dtos = await ctx.ProjectFromDataVoAsync<AdvancedRow, AdvancedRowDto>(
                selector: r => new AdvancedRowDto(r.Name, r.Score ?? 0));

            Assert.Equal(2, dtos.Count);
            Assert.Contains(dtos, d => d.Name == "Gamma");
            Assert.Contains(dtos, d => d.Name == "Delta");
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public void ProjectFromDataVo_WithQueryShape_ReturnsFilteredProjection()
    {
        string db = $"datavo_proj_filtered_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var seed = CreateContext(cs))
            {
                seed.Database.EnsureCreated();
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "Active1", Score = 8.0, IsActive = true });
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "Inactive1", Score = 3.0, IsActive = false });
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "Active2", Score = 6.5, IsActive = true });
                seed.SaveChanges();
            }

            using var ctx = CreateContext(cs);
            var dtos = ctx.ProjectFromDataVo<AdvancedRow, AdvancedRowDto>(
                selector: r => new AdvancedRowDto(r.Name, r.Score ?? 0),
                queryShape: q => q.Where(r => r.IsActive).OrderByDescending(r => r.Score));

            Assert.Equal(2, dtos.Count);
            Assert.All(dtos, d => Assert.True(d.Value > 5.0));
            Assert.Equal("Active1", dtos[0].Name);  // 8.0 first after desc ordering
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public void ProjectFromDataVo_NullSelector_ThrowsArgumentNullException()
    {
        string cs = $"StorageMode=InMemory;DataSource=proj_null_{Guid.NewGuid():N}";
        using var ctx = CreateContext(cs);

        Assert.Throws<ArgumentNullException>(() =>
            ctx.ProjectFromDataVo<AdvancedRow, AdvancedRowDto>(selector: null!));
    }

    [Fact]
    public async Task ProjectFromDataVoAsync_NullSelector_ThrowsArgumentNullException()
    {
        string cs = $"StorageMode=InMemory;DataSource=proj_null_async_{Guid.NewGuid():N}";
        using var ctx = CreateContext(cs);

        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            ctx.ProjectFromDataVoAsync<AdvancedRow, AdvancedRowDto>(selector: null!));
    }

    [Fact]
    public void ProjectFromDataVo_BlockedShapeOperator_ThrowsDataVoEfException()
    {
        string db = $"datavo_proj_blocked_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var seed = CreateContext(cs))
            {
                seed.Database.EnsureCreated();
                seed.AdvancedRows.Add(new AdvancedRow
                {
                    Id = Guid.NewGuid(),
                    Name = "Seed",
                    Score = 1.0,
                    IsActive = true
                });
                seed.SaveChanges();
            }

            using var ctx = CreateContext(cs);

            var ex = Assert.Throws<DataVoEfException>(() =>
                ctx.ProjectFromDataVo<AdvancedRow, AdvancedRowDto>(
                    selector: r => new AdvancedRowDto(r.Name, r.Score ?? 0),
                    queryShape: q => q.GroupBy(r => r.IsActive).SelectMany(g => g)));

            Assert.Equal(DataVoEfOperation.Query, ex.Operation);
            Assert.Contains("GroupBy", ex.Message);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public void GetActiveProviderModeStatus_DefaultMode_IsBridgeOnly()
    {
        string cs = $"StorageMode=InMemory;DataSource=mode_default_{Guid.NewGuid():N}";
        using var ctx = CreateContext(cs);

        var mode = ctx.GetActiveProviderModeStatus();

        Assert.Equal(DataVoProviderMode.BridgeOnly, mode.Mode);
        Assert.True(mode.IsBridgeOnlyMode);
        Assert.False(mode.ProviderIdentityPreviewEnabled);
        Assert.False(mode.NativeQueryTranslationPreviewEnabled);
    }

    [Fact]
    public void GetActiveProviderModeStatus_ProviderIdentityPreview_IsReported()
    {
        string cs = $"StorageMode=InMemory;DataSource=mode_identity_{Guid.NewGuid():N}";
        using var ctx = CreateContext(cs, o => o.EnableProviderIdentityPreview());

        var mode = ctx.GetActiveProviderModeStatus();

        Assert.Equal(DataVoProviderMode.ProviderIdentityPreview, mode.Mode);
        Assert.True(mode.ProviderIdentityPreviewEnabled);
        Assert.False(mode.NativeQueryTranslationPreviewEnabled);
    }

    [Fact]
    public void ExplainQueryFromDataVo_WhereOrderByTake_WithTranslationPreview_ReportsNativePreviewAndQueryExecutes()
    {
        string db = $"datavo_translate_preview_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var seed = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview()))
            {
                seed.Database.EnsureCreated();
                seed.ExecuteSqlOnDataVo("INSERT INTO Boards (Id, Name) VALUES (1, 'A');");
                seed.ExecuteSqlOnDataVo("INSERT INTO Boards (Id, Name) VALUES (2, 'B');");
                seed.ExecuteSqlOnDataVo("INSERT INTO Boards (Id, Name) VALUES (3, 'C');");
            }

            using var ctx = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview());

            var diagnostics = ctx.ExplainQueryFromDataVo<Board>(q =>
                q.Where(b => b.Id >= 2)
                 .OrderByDescending(b => b.Name)
                 .Take(1));

            Assert.Equal(DataVoProviderMode.NativeTranslationPreview, diagnostics.ProviderMode);
            Assert.Equal(DataVoQueryTranslationOutcome.NativeTranslationPreview, diagnostics.Outcome);
            Assert.Empty(diagnostics.FallbackReasons);
            Assert.Empty(diagnostics.BlockedReasons);
            Assert.Contains("Where", diagnostics.Operators);
            Assert.Contains("OrderByDescending", diagnostics.Operators);
            Assert.Contains("Take", diagnostics.Operators);

            var rows = ctx.QueryFromDataVo<Board>(q =>
                q.Where(b => b.Id >= 2)
                 .OrderByDescending(b => b.Name)
                 .Take(1));

            var row = Assert.Single(rows);
            Assert.Equal("C", row.Name);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public void ExplainQueryFromDataVo_Include_WithTranslationPreview_ReportsGuardedFallbackAndQueryExecutes()
    {
        string db = $"datavo_translate_fallback_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var seed = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview()))
            {
                seed.Database.EnsureCreated();
                seed.ExecuteSqlOnDataVo("INSERT INTO Boards (Id, Name) VALUES (1, 'Root');");
                seed.ExecuteSqlOnDataVo("INSERT INTO Cards (Id, BoardId, Title) VALUES (10, 1, 'Child');");
            }

            using var ctx = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview());

            var diagnostics = ctx.ExplainQueryFromDataVo<Card>(q =>
                q.Include(c => c.Board)
                 .Where(c => c.Id == 10));

            Assert.Equal(DataVoQueryTranslationOutcome.GuardedFallback, diagnostics.Outcome);
            Assert.Contains(diagnostics.FallbackReasons, r => r.Contains("Include", StringComparison.OrdinalIgnoreCase));
            Assert.Empty(diagnostics.BlockedReasons);

            var rows = ctx.QueryFromDataVo<Card>(q =>
                q.Include(c => c.Board)
                 .Where(c => c.Id == 10));

            var row = Assert.Single(rows);
            Assert.NotNull(row.Board);
            Assert.Equal("Root", row.Board!.Name);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public void ExplainQueryFromDataVo_GroupBy_WithTranslationPreview_ReportsBlocked_AndExecutionThrows()
    {
        string db = $"datavo_translate_blocked_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var seed = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview()))
            {
                seed.Database.EnsureCreated();
                seed.ExecuteSqlOnDataVo("INSERT INTO Boards (Id, Name) VALUES (1, 'One');");
            }

            using var ctx = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview());

            var diagnostics = ctx.ExplainQueryFromDataVo<Board>(q =>
                q.GroupBy(b => b.Name).Select(g => g.First()));

            Assert.Equal(DataVoQueryTranslationOutcome.Blocked, diagnostics.Outcome);
            Assert.Contains(diagnostics.BlockedReasons, r => r.Contains("GroupBy", StringComparison.OrdinalIgnoreCase));

            var ex = Assert.Throws<DataVoEfException>(() =>
                ctx.QueryFromDataVo<Board>(q => q.GroupBy(b => b.Name).Select(g => g.First())));

            Assert.Equal(DataVoEfOperation.Query, ex.Operation);
            Assert.Contains("GroupBy", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public void DatabaseFacade_ExplainDataVoQuery_UsesLiveContextMode()
    {
        string cs = $"StorageMode=InMemory;DataSource=facade_explain_{Guid.NewGuid():N}";
        using var ctx = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview());

        var diagnostics = ctx.Database.ExplainDataVoQuery<Board>(q => q.Where(b => b.Id > 0).Take(5));

        Assert.Equal(DataVoProviderMode.NativeTranslationPreview, diagnostics.ProviderMode);
        Assert.Equal(DataVoQueryTranslationOutcome.NativeTranslationPreview, diagnostics.Outcome);
    }

    [Fact]
    public void DatabaseFacade_ExplainDataVoQuery_ThrowsForNonDataVoContext()
    {
        var options = new DbContextOptionsBuilder<PlainContext>()
            .UseInMemoryDatabase($"plain_explain_{Guid.NewGuid():N}")
            .UseDataVo($"StorageMode=InMemory;DataSource=plain_explain_{Guid.NewGuid():N}")
            .Options;

        using var ctx = new PlainContext(options);

        Assert.Throws<InvalidOperationException>(() =>
            ctx.Database.ExplainDataVoQuery<PlainRow>(q => q.Where(r => r.Id > 0)));
    }

    [Fact]
    public void QueryFromDataVo_WhereOrderBySkipTake_WithTranslationPreview_ExecutesViaNativeSubset()
    {
        string db = $"datavo_translate_skip_take_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var seed = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview()))
            {
                seed.Database.EnsureCreated();
                seed.ExecuteSqlOnDataVo("INSERT INTO Boards (Id, Name) VALUES (1, 'A');");
                seed.ExecuteSqlOnDataVo("INSERT INTO Boards (Id, Name) VALUES (2, 'B');");
                seed.ExecuteSqlOnDataVo("INSERT INTO Boards (Id, Name) VALUES (3, 'C');");
                seed.ExecuteSqlOnDataVo("INSERT INTO Boards (Id, Name) VALUES (4, 'D');");
            }

            using var ctx = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview());

            var diagnostics = ctx.ExplainQueryFromDataVo<Board>(q =>
                q.Where(b => b.Id >= 2)
                 .OrderBy(b => b.Id)
                 .Skip(1)
                 .Take(1));

            Assert.Equal(DataVoQueryTranslationOutcome.NativeTranslationPreview, diagnostics.Outcome);
            Assert.Contains("Skip", diagnostics.Operators);
            Assert.Contains("Take", diagnostics.Operators);

            var rows = ctx.QueryFromDataVo<Board>(q =>
                q.Where(b => b.Id >= 2)
                 .OrderBy(b => b.Id)
                 .Skip(1)
                 .Take(1));

            var row = Assert.Single(rows);
            Assert.Equal(3, row.Id);
            Assert.Equal("C", row.Name);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public void ExplainProjectFromDataVo_SimpleSelect_WithTranslationPreview_ReportsNativePreview()
    {
        string cs = $"StorageMode=InMemory;DataSource=proj_diag_{Guid.NewGuid():N}";
        using var ctx = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview());

        var diagnostics = ctx.ExplainProjectFromDataVo<AdvancedRow, NativeRowProjection>(
            selector: row => new NativeRowProjection(row.Name, row.Score),
            queryShape: q => q.Where(row => row.Score > 0).OrderBy(row => row.Name).Skip(1).Take(2));

        Assert.Equal(DataVoQueryTranslationOutcome.NativeTranslationPreview, diagnostics.Outcome);
        Assert.Contains("Select", diagnostics.Operators);
        Assert.Contains("Skip", diagnostics.Operators);
        Assert.Contains("Take", diagnostics.Operators);
        Assert.Empty(diagnostics.FallbackReasons);
        Assert.Empty(diagnostics.BlockedReasons);
    }

    [Fact]
    public void ExplainProjectFromDataVo_ComplexSelect_WithTranslationPreview_ReportsNativePreview()
    {
        string cs = $"StorageMode=InMemory;DataSource=proj_fallback_diag_{Guid.NewGuid():N}";
        using var ctx = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview());

        var diagnostics = ctx.ExplainProjectFromDataVo<AdvancedRow, AdvancedRowDto>(
            selector: row => new AdvancedRowDto(row.Name, (row.Score ?? 0) + 1),
            queryShape: q => q.Where(row => row.Score > 0));

        Assert.Equal(DataVoQueryTranslationOutcome.NativeTranslationPreview, diagnostics.Outcome);
        Assert.Empty(diagnostics.FallbackReasons);
        Assert.Empty(diagnostics.BlockedReasons);
    }

    [Fact]
    public void ExplainQueryFromDataVo_StringMethodsInWhere_WithTranslationPreview_ReportsNativePreview()
    {
        string cs = $"StorageMode=InMemory;DataSource=where_string_diag_{Guid.NewGuid():N}";
        using var ctx = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview());

        var diagnostics = ctx.ExplainQueryFromDataVo<AdvancedRow>(q =>
            q.Where(row => row.Name.Contains("A") || row.Name.StartsWith("B") || row.Name.EndsWith("z")));

        Assert.Equal(DataVoQueryTranslationOutcome.NativeTranslationPreview, diagnostics.Outcome);
        Assert.Contains("Where", diagnostics.Operators);
        Assert.Empty(diagnostics.FallbackReasons);
        Assert.Empty(diagnostics.BlockedReasons);
    }

    [Fact]
    public void QueryFromDataVo_StringMethodsInWhere_WithTranslationPreview_ExecutesViaNativeSubset()
    {
        string db = $"datavo_native_where_string_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var seed = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview()))
            {
                seed.Database.EnsureCreated();
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "Alpha", Score = 1, IsActive = true });
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "Beta", Score = 1, IsActive = true });
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "GammaZ", Score = 1, IsActive = true });
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "NoMatch", Score = 1, IsActive = true });
                seed.SaveChanges();
            }

            using var ctx = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview());

            var rows = ctx.QueryFromDataVo<AdvancedRow>(q =>
                q.Where(row => row.Name.Contains("ph") || row.Name.StartsWith("Be") || row.Name.EndsWith("Z"))
                 .OrderBy(row => row.Name));

            Assert.Equal(3, rows.Count);
            Assert.Equal("Alpha", rows[0].Name);
            Assert.Equal("Beta", rows[1].Name);
            Assert.Equal("GammaZ", rows[2].Name);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public void ExplainQueryFromDataVo_CoalescePredicate_WithTranslationPreview_ReportsNativePreview()
    {
        string cs = $"StorageMode=InMemory;DataSource=where_coalesce_diag_{Guid.NewGuid():N}";
        using var ctx = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview());

        var diagnostics = ctx.ExplainQueryFromDataVo<AdvancedRow>(q =>
            q.Where(row => (row.Score ?? 0) >= 5));

        Assert.Equal(DataVoQueryTranslationOutcome.NativeTranslationPreview, diagnostics.Outcome);
        Assert.Contains("Where", diagnostics.Operators);
        Assert.Empty(diagnostics.FallbackReasons);
        Assert.Empty(diagnostics.BlockedReasons);
    }

    [Fact]
    public void QueryFromDataVo_CoalescePredicate_WithTranslationPreview_ExecutesViaNativeSubset()
    {
        string db = $"datavo_native_where_coalesce_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var seed = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview()))
            {
                seed.Database.EnsureCreated();
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "A", Score = null, IsActive = true });
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "B", Score = 4.9, IsActive = true });
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "C", Score = 5.0, IsActive = true });
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "D", Score = 7.5, IsActive = true });
                seed.SaveChanges();
            }

            using var ctx = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview());

            var rows = ctx.QueryFromDataVo<AdvancedRow>(q =>
                q.Where(row => (row.Score ?? 0) >= 5)
                 .OrderBy(row => row.Name));

            Assert.Equal(2, rows.Count);
            Assert.Equal("C", rows[0].Name);
            Assert.Equal("D", rows[1].Name);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public void ProjectFromDataVo_SimpleSelect_WithTranslationPreview_ExecutesAndReturnsProjectedRows()
    {
        string db = $"datavo_native_project_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var seed = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview()))
            {
                seed.Database.EnsureCreated();
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "P1", Score = 5.5, Level = Priority.High, IsActive = true });
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "P2", Score = 6.0, Level = Priority.Medium, IsActive = true });
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "P3", Score = 2.0, Level = Priority.Low, IsActive = false });
                seed.SaveChanges();
            }

            using var ctx = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview());

            var rows = ctx.ProjectFromDataVo<AdvancedRow, NativeRowProjection>(
                selector: row => new NativeRowProjection(row.Name, row.Score),
                queryShape: q => q.Where(row => row.Score > 0).OrderBy(row => row.Name).Skip(1).Take(1));

            var row = Assert.Single(rows);
            Assert.Equal("P2", row.Name);
            Assert.Equal(6.0, row.Score);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public void ProjectFromDataVo_ComputedSelect_WithTranslationPreview_ExecutesAndReturnsProjectedRows()
    {
        string db = $"datavo_native_project_computed_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var seed = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview()))
            {
                seed.Database.EnsureCreated();
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "A", Score = 2.0, Level = Priority.Low, IsActive = true });
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "B", Score = null, Level = Priority.Medium, IsActive = true });
                seed.SaveChanges();
            }

            using var ctx = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview());

            var rows = ctx.ProjectFromDataVo<AdvancedRow, AdvancedRowDto>(
                selector: row => new AdvancedRowDto(row.Name, (row.Score ?? 0) + 1),
                queryShape: q => q.OrderBy(row => row.Name));

            Assert.Equal(2, rows.Count);
            Assert.Equal("A", rows[0].Name);
            Assert.Equal(3.0, rows[0].Value);
            Assert.Equal("B", rows[1].Name);
            Assert.Equal(1.0, rows[1].Value);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public void ExplainQueryFromDataVo_NestedPredicateMix_WithTranslationPreview_ReportsNativePreview()
    {
        string cs = $"StorageMode=InMemory;DataSource=where_nested_diag_{Guid.NewGuid():N}";
        using var ctx = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview());

        var diagnostics = ctx.ExplainQueryFromDataVo<AdvancedRow>(q =>
            q.Where(row =>
                (row.Name.StartsWith("A") && (row.Score ?? 0) >= 5)
                || row.Name.EndsWith("z")));

        Assert.Equal(DataVoQueryTranslationOutcome.NativeTranslationPreview, diagnostics.Outcome);
        Assert.Contains("Where", diagnostics.Operators);
        Assert.Empty(diagnostics.FallbackReasons);
        Assert.Empty(diagnostics.BlockedReasons);
    }

    [Fact]
    public void QueryFromDataVo_NestedPredicateMix_WithTranslationPreview_ExecutesViaNativeSubset()
    {
        string db = $"datavo_native_where_nested_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var seed = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview()))
            {
                seed.Database.EnsureCreated();
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "Atlas", Score = 6.0, IsActive = true, Level = Priority.High });
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "Fizz", Score = 1.0, IsActive = false, Level = Priority.Low });
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "Aster", Score = 2.0, IsActive = true, Level = Priority.Medium });
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "Bolt", Score = 9.0, IsActive = true, Level = Priority.High });
                seed.SaveChanges();
            }

            using var ctx = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview());

            var rows = ctx.QueryFromDataVo<AdvancedRow>(q =>
                q.Where(row =>
                        (row.Name.StartsWith("A") && (row.Score ?? 0) >= 5)
                        || row.Name.EndsWith("z"))
                 .OrderBy(row => row.Name));

            Assert.Equal(2, rows.Count);
            Assert.Equal("Atlas", rows[0].Name);
            Assert.Equal("Fizz", rows[1].Name);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public void QueryFromDataVo_StringStartsWithEmptyPrefix_WithTranslationPreview_ExecutesViaNativeSubset()
    {
        string db = $"datavo_native_where_quote_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var seed = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview()))
            {
                seed.Database.EnsureCreated();
                seed.Boards.Add(new Board { Id = 1, Name = "Alpha" });
                seed.Boards.Add(new Board { Id = 2, Name = "Beta" });
                seed.SaveChanges();
            }

            using var ctx = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview());

            var rows = ctx.QueryFromDataVo<Board>(q =>
                q.Where(row => row.Name.StartsWith(string.Empty))
                 .OrderBy(row => row.Id));

            Assert.Equal(2, rows.Count);
            Assert.Equal("Alpha", rows[0].Name);
            Assert.Equal("Beta", rows[1].Name);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public void QueryFromDataVo_CoalesceOnRightComparison_WithTranslationPreview_ExecutesViaNativeSubset()
    {
        string db = $"datavo_native_where_coalesce_right_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var seed = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview()))
            {
                seed.Database.EnsureCreated();
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "A", Score = null, IsActive = true });
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "B", Score = 4.99, IsActive = true });
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "C", Score = 5.00, IsActive = true });
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "D", Score = 6.10, IsActive = true });
                seed.SaveChanges();
            }

            using var ctx = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview());

            var rows = ctx.QueryFromDataVo<AdvancedRow>(q =>
                q.Where(row => 5 <= (row.Score ?? 0))
                 .OrderBy(row => row.Name));

            Assert.Equal(2, rows.Count);
            Assert.Equal("C", rows[0].Name);
            Assert.Equal("D", rows[1].Name);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public void ExplainQueryFromDataVo_UnsupportedStringMethod_WithTranslationPreview_ReportsGuardedFallback()
    {
        string cs = $"StorageMode=InMemory;DataSource=where_tolower_diag_{Guid.NewGuid():N}";
        using var ctx = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview());

        var diagnostics = ctx.ExplainQueryFromDataVo<AdvancedRow>(q =>
            q.Where(row => row.Name.ToLower() == "alpha"));

        Assert.Equal(DataVoQueryTranslationOutcome.GuardedFallback, diagnostics.Outcome);
        Assert.Contains(diagnostics.FallbackReasons, reason => reason.Contains("Where", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void QueryFromDataVo_UnsupportedStringMethod_WithTranslationPreview_FallsBackAndExecutes()
    {
        string db = $"datavo_where_tolower_fallback_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var seed = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview()))
            {
                seed.Database.EnsureCreated();
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "Alpha", Score = 1, IsActive = true });
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "Beta", Score = 1, IsActive = true });
                seed.SaveChanges();
            }

            using var ctx = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview());

            var rows = ctx.QueryFromDataVo<AdvancedRow>(q =>
                q.Where(row => row.Name.ToLower() == "alpha"));

            var row = Assert.Single(rows);
            Assert.Equal("Alpha", row.Name);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public void ExplainQueryFromDataVo_CapturedSkipTakeAndThenBy_WithTranslationPreview_ReportsNativePreview()
    {
        string cs = $"StorageMode=InMemory;DataSource=where_capture_paging_diag_{Guid.NewGuid():N}";
        using var ctx = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview());

        int skip = 1;
        int take = 2;

        var diagnostics = ctx.ExplainQueryFromDataVo<Board>(q =>
            q.OrderBy(row => row.Name)
             .ThenByDescending(row => row.Id)
             .Skip(skip)
             .Take(take));

        Assert.Equal(DataVoQueryTranslationOutcome.NativeTranslationPreview, diagnostics.Outcome);
        Assert.Contains("ThenByDescending", diagnostics.Operators);
        Assert.Contains("Skip", diagnostics.Operators);
        Assert.Contains("Take", diagnostics.Operators);
    }

    [Fact]
    public void QueryFromDataVo_CapturedSkipTakeAndThenBy_WithTranslationPreview_ExecutesViaNativeSubset()
    {
        string db = $"datavo_native_capture_paging_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var seed = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview()))
            {
                seed.Database.EnsureCreated();
                seed.ExecuteSqlOnDataVo("INSERT INTO Boards (Id, Name) VALUES (1, 'K');");
                seed.ExecuteSqlOnDataVo("INSERT INTO Boards (Id, Name) VALUES (2, 'A');");
                seed.ExecuteSqlOnDataVo("INSERT INTO Boards (Id, Name) VALUES (3, 'A');");
                seed.ExecuteSqlOnDataVo("INSERT INTO Boards (Id, Name) VALUES (4, 'Z');");
                seed.ExecuteSqlOnDataVo("INSERT INTO Boards (Id, Name) VALUES (5, 'M');");
            }

            using var ctx = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview());

            int skip = 1;
            int take = 2;
            var rows = ctx.QueryFromDataVo<Board>(q =>
                q.OrderBy(row => row.Name)
                 .ThenByDescending(row => row.Id)
                 .Skip(skip)
                 .Take(take));

            Assert.Equal(2, rows.Count);
            Assert.Equal(2, rows[0].Id);
            Assert.Equal("A", rows[0].Name);
            Assert.Equal(1, rows[1].Id);
            Assert.Equal("K", rows[1].Name);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public void ExplainProjectFromDataVo_MethodAndConditionalProjection_WithTranslationPreview_ReportsNativePreview()
    {
        string cs = $"StorageMode=InMemory;DataSource=proj_complex_diag_{Guid.NewGuid():N}";
        using var ctx = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview());

        var diagnostics = ctx.ExplainProjectFromDataVo<AdvancedRow, ComplexProjection>(
            selector: row => new ComplexProjection(
                row.Name.ToUpper(),
                Math.Round((row.Score ?? 0) + (row.IsActive ? 10 : 0), 2),
                (row.Score ?? 0) >= 7 && row.Level == Priority.High),
            queryShape: q => q.Where(row => row.Name.Contains("A") || row.Name.Contains("B"))
                              .OrderBy(row => row.Name));

        Assert.Equal(DataVoQueryTranslationOutcome.NativeTranslationPreview, diagnostics.Outcome);
        Assert.Contains("Select", diagnostics.Operators);
        Assert.Empty(diagnostics.FallbackReasons);
        Assert.Empty(diagnostics.BlockedReasons);
    }

    [Fact]
    public void ProjectFromDataVo_MethodAndConditionalProjection_WithTranslationPreview_ExecutesAndReturnsProjectedRows()
    {
        string db = $"datavo_native_project_complex_{Guid.NewGuid():N}";
        string cs = $"StorageMode=Disk;DataSource={db}";
        string path = Path.Combine(Directory.GetCurrentDirectory(), db);

        try
        {
            using (var seed = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview()))
            {
                seed.Database.EnsureCreated();
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "alpha", Score = 8.25, IsActive = true, Level = Priority.High });
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "beta", Score = 4.0, IsActive = false, Level = Priority.Medium });
                seed.AdvancedRows.Add(new AdvancedRow { Id = Guid.NewGuid(), Name = "gamma", Score = null, IsActive = true, Level = Priority.Low });
                seed.SaveChanges();
            }

            using var ctx = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview());

            var rows = ctx.ProjectFromDataVo<AdvancedRow, ComplexProjection>(
                selector: row => new ComplexProjection(
                    row.Name.ToUpper(),
                    Math.Round((row.Score ?? 0) + (row.IsActive ? 10 : 0), 2),
                    (row.Score ?? 0) >= 7 && row.Level == Priority.High),
                queryShape: q => q.Where(row => row.Name.Contains("a"))
                                  .OrderBy(row => row.Name));

            Assert.Equal(3, rows.Count);
            Assert.Equal("ALPHA", rows[0].NameUpper);
            Assert.Equal(18.25, rows[0].ComputedScore);
            Assert.True(rows[0].IsTopTier);

            Assert.Equal("BETA", rows[1].NameUpper);
            Assert.Equal(4.0, rows[1].ComputedScore);
            Assert.False(rows[1].IsTopTier);

            Assert.Equal("GAMMA", rows[2].NameUpper);
            Assert.Equal(10.0, rows[2].ComputedScore);
            Assert.False(rows[2].IsTopTier);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public void DatabaseFacade_ExplainDataVoProjectionQuery_UsesLiveContextMode()
    {
        string cs = $"StorageMode=InMemory;DataSource=facade_proj_explain_{Guid.NewGuid():N}";
        using var ctx = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview());

        var diagnostics = ctx.Database.ExplainDataVoProjectionQuery<AdvancedRow, NativeRowProjection>(
            selector: row => new NativeRowProjection(row.Name, row.Score),
            queryShape: q => q.Where(row => row.Score > 0).Take(5));

        Assert.Equal(DataVoProviderMode.NativeTranslationPreview, diagnostics.ProviderMode);
        Assert.Equal(DataVoQueryTranslationOutcome.NativeTranslationPreview, diagnostics.Outcome);
    }

    [Fact]
    public void DatabaseFacade_ExplainDataVoProjectionQuery_ThrowsForNonDataVoContext()
    {
        var options = new DbContextOptionsBuilder<PlainContext>()
            .UseInMemoryDatabase($"plain_proj_explain_{Guid.NewGuid():N}")
            .UseDataVo($"StorageMode=InMemory;DataSource=plain_proj_explain_{Guid.NewGuid():N}")
            .Options;

        using var ctx = new PlainContext(options);

        Assert.Throws<InvalidOperationException>(() =>
            ctx.Database.ExplainDataVoProjectionQuery<PlainRow, int>(
                selector: row => row.Id,
                queryShape: q => q.Where(row => row.Id > 0)));
    }

    [Fact]
    public void ProviderIdentityParityReport_ExposesMetadataAndCapabilities()
    {
        string cs = $"StorageMode=InMemory;DataSource=parity_{Guid.NewGuid():N}";
        using var ctx = CreateContext(cs, o => o.EnableNativeQueryTranslationPreview());

        var report = ctx.GetProviderIdentityParityReport();

        Assert.True(report.HostProviderConfigured);
        Assert.Contains("InMemory", report.HostProviderName, StringComparison.OrdinalIgnoreCase);
        Assert.True(report.MetadataParitySatisfied);
        Assert.Empty(report.MetadataWarnings);
        Assert.Contains("Where", report.NativePreviewOperators);
        Assert.Contains("Skip", report.NativePreviewOperators);
        Assert.Contains("Select", report.NativePreviewOperators);
        Assert.Contains("GroupBy", report.BlockedOperators);
        Assert.Equal(DataVoProviderMode.NativeTranslationPreview, report.ModeStatus.Mode);

        var facadeReport = ctx.Database.GetDataVoProviderIdentityParityReport();
        Assert.Equal(report.ModeStatus.Mode, facadeReport.ModeStatus.Mode);
        Assert.Equal(report.QueryableEntityTypeCount, facadeReport.QueryableEntityTypeCount);
    }

    [Fact]
    public void DatabaseFacade_GetDataVoProviderIdentityParityReport_ThrowsForNonDataVoContext()
    {
        var options = new DbContextOptionsBuilder<PlainContext>()
            .UseInMemoryDatabase($"plain_parity_{Guid.NewGuid():N}")
            .UseDataVo($"StorageMode=InMemory;DataSource=plain_parity_{Guid.NewGuid():N}")
            .Options;

        using var ctx = new PlainContext(options);

        Assert.Throws<InvalidOperationException>(() => ctx.Database.GetDataVoProviderIdentityParityReport());
    }

    private static AdvancedContext CreateContext(
        string connectionString,
        Action<DataVoDbContextOptionsBuilder>? dataVoOptionsAction = null)
    {
        var options = new DbContextOptionsBuilder<AdvancedContext>()
            .UseInMemoryDatabase($"ef_adv_{Guid.NewGuid():N}")
            .UseDataVo(connectionString, dataVoOptionsAction)
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

    private sealed record AdvancedRowDto(string Name, double Value);

    private sealed record NativeRowProjection(string Name, double? Score);

    private sealed record ComplexProjection(string NameUpper, double ComputedScore, bool IsTopTier);

    private sealed class PlainContext(DbContextOptions<PlainContext> options) : DbContext(options)
    {
        public DbSet<PlainRow> PlainRows => Set<PlainRow>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<PlainRow>(e =>
            {
                e.ToTable("PlainRows");
                e.HasKey(r => r.Id);
            });
        }
    }

    private sealed class PlainRow { public int Id { get; set; } }

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
