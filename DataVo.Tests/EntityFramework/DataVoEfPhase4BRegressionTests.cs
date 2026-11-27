using System.ComponentModel.DataAnnotations.Schema;
using DataVo.Core.Annotations;
using DataVo.Core.Contracts.Results;
using DataVo.Data;
using DataVo.EntityFrameworkCore;
using DataVo.EntityFrameworkCore.Infrastructure;
using DataVo.Tests.BrowserParity;
using Microsoft.EntityFrameworkCore;

namespace DataVo.Tests.EntityFramework;

/// <summary>
/// Regression tests for the EF bridge fixes: shared in-memory storage, transactional SaveChanges,
/// parameterized SQL with full DATETIME values, vector/constraint DDL, immutable options and native
/// translation parity.
/// </summary>
[BrowserTranslateIgnore("EntityFramework provider-bridge tests rely on EF runtime semantics and are validated in .NET lane.")]
public class DataVoEfPhase4BRegressionTests
{
    // ------------------------------------------------------------------ LE-24: UseInMemoryStorage

    [Fact]
    public void UseInMemoryStorage_KeepsDataAcrossBridgeCallsAndContexts()
    {
        string dataSource = NewInMemoryDataSource();

        using (var writer = CreateGadgetContext(InMemory(dataSource)))
        {
            Assert.True(writer.Database.EnsureCreated());
            writer.Add(NewGadget(1, "first"));
            writer.SaveChanges();
            writer.ExecuteSqlOnDataVo("INSERT INTO Gadgets (Id, Name, Age, CreatedAt, SeenAt, Alarm, Day, Shade, Grade) VALUES (@id, @name, 3, '2026-01-01', '2026-01-01', '2026-01-01', '2026-01-01', 1, 'B');",
                ("@id", 2), ("@name", "raw"));
            Assert.False(writer.Database.EnsureCreated());
        }

        using var reader = CreateGadgetContext(InMemory(dataSource));
        reader.LoadFromDataVo();

        Assert.Equal(["first", "raw"], reader.Gadgets.OrderBy(static g => g.Id).Select(static g => g.Name).ToList());
        Assert.Equal(2, reader.CountFromDataVo<Gadget>());
        Assert.Single(reader.QueryFromDataVo<Gadget>(static q => q.Where(static g => g.Name == "raw")));
    }

    [Fact]
    public void UseInMemoryStorage_DifferentDataSourcesAreIsolated()
    {
        using var first = CreateGadgetContext(InMemory(NewInMemoryDataSource()));
        using var second = CreateGadgetContext(InMemory(NewInMemoryDataSource()));

        first.Add(NewGadget(1, "only-in-first"));
        first.SaveChanges();

        Assert.Equal(1, first.CountFromDataVo<Gadget>());
        second.Database.EnsureCreated();
        Assert.Equal(0, second.CountFromDataVo<Gadget>());
    }

    // ------------------------------------------------------------------ LE-25: transactional SaveChanges

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void SaveChanges_FailingInsert_RollsBackEarlierStatements(bool useLsm)
    {
        var connection = useLsm ? Lsm(TestStorageDirectories.Create("ef_tx_insert")) : InMemory(NewInMemoryDataSource());

        using (var seed = CreateGadgetContext(connection))
        {
            seed.Add(NewGadget(2, "existing"));
            seed.SaveChanges();
        }

        using var context = CreateGadgetContext(connection);
        context.Add(NewGadget(1, "new"));
        context.Add(NewGadget(2, "duplicate"));

        var ex = Assert.Throws<DataVoEfException>(() => context.SaveChanges());
        Assert.Equal(DataVoEfOperation.Insert, ex.Operation);
        Assert.Equal(DataVoErrorCodes.PrimaryKeyViolation, ex.ErrorCode);

        // Gadget 1 was inserted before the failing statement, but nothing was committed.
        Assert.Equal(["existing"], context.QueryFromDataVo<Gadget>().Select(static g => g.Name).ToList());
    }

    [Fact]
    public void SaveChanges_FailingUpdate_ReportsUpdateOperationAndRollsBackInsert()
    {
        var connection = InMemory(NewInMemoryDataSource());

        using (var seed = CreateGadgetContext(connection))
        {
            seed.Add(NewGadget(1, "a"));
            seed.Add(NewGadget(2, "b"));
            seed.SaveChanges();
        }

        using var context = CreateGadgetContext(connection);
        context.LoadFromDataVo();
        context.Add(NewGadget(3, "c"));
        context.Gadgets.Single(static g => g.Id == 2).Name = "a"; // Name is unique

        var ex = Assert.Throws<DataVoEfException>(() => context.SaveChanges());
        Assert.Equal(DataVoEfOperation.Update, ex.Operation);
        Assert.Equal(DataVoErrorCodes.UniqueViolation, ex.ErrorCode);

        Assert.Equal(2, context.CountFromDataVo<Gadget>());
        Assert.Equal("b", context.FirstOrDefaultFromDataVo<Gadget>(static g => g.Id == 2)!.Name);
    }

    // ------------------------------------------------------------------ LE-26: parameters and DATETIME

    [Fact]
    public void SaveChanges_RoundTripsFullTimestampsAndHostileStrings()
    {
        var connection = InMemory(NewInMemoryDataSource());
        var created = new DateTime(2026, 11, 26, 13, 45, 12, DateTimeKind.Utc).AddTicks(1234567);
        var seen = new DateTimeOffset(2026, 11, 26, 15, 0, 1, TimeSpan.FromHours(2)).AddTicks(42);
        const string hostile = "O'Brien'); DELETE FROM Gadgets; --";

        using (var writer = CreateGadgetContext(connection))
        {
            var gadget = NewGadget(1, hostile);
            gadget.CreatedAt = created;
            gadget.SeenAt = seen;
            gadget.Alarm = new TimeOnly(23, 59, 58, 123);
            gadget.Day = new DateOnly(2026, 2, 28);
            gadget.Shade = Shade.Wide;
            gadget.Grade = 'Z';
            writer.Add(gadget);
            writer.Add(NewGadget(2, "other"));
            writer.SaveChanges();
        }

        using var reader = CreateGadgetContext(connection);
        reader.LoadFromDataVo();
        Gadget loaded = reader.Gadgets.Single(static g => g.Id == 1);

        Assert.Equal(hostile, loaded.Name);
        Assert.Equal(created, loaded.CreatedAt);
        Assert.Equal(seen, loaded.SeenAt);
        Assert.Equal(new TimeOnly(23, 59, 58, 123), loaded.Alarm);
        Assert.Equal(new DateOnly(2026, 2, 28), loaded.Day);
        Assert.Equal(Shade.Wide, loaded.Shade);
        Assert.Equal('Z', loaded.Grade);
        Assert.Equal(2, reader.CountFromDataVo<Gadget>());

        string script = reader.GenerateDataVoCreateScript();
        Assert.Contains("CreatedAt DATETIME NOT NULL", script, StringComparison.Ordinal);
        Assert.Contains("SeenAt DATETIME NOT NULL", script, StringComparison.Ordinal);
        Assert.Contains("Alarm DATETIME NOT NULL", script, StringComparison.Ordinal);
        Assert.Contains("Day DATE NOT NULL", script, StringComparison.Ordinal);
        Assert.Contains("Shade BIGINT NOT NULL", script, StringComparison.Ordinal);
    }

    [Fact]
    public void NativeQuery_BindsTimestampsAndEscapesLikeWildcards()
    {
        var connection = InMemory(NewInMemoryDataSource());
        var noon = new DateTime(2026, 11, 26, 12, 0, 0, DateTimeKind.Utc);

        using (var writer = CreateGadgetContext(connection))
        {
            string[] names = ["50% off", "50 percent", "a_b", "axb", "back\\slash"];
            for (int i = 0; i < names.Length; i++)
            {
                var gadget = NewGadget(i + 1, names[i]);
                gadget.CreatedAt = noon.AddMinutes(i - 2); // 11:58 .. 12:02
                writer.Add(gadget);
            }

            writer.SaveChanges();
        }

        using var context = CreateGadgetContext(connection, native: true);

        Assert.Equal(["50% off"], NativeNames(context, static q => q.Where(static g => g.Name.Contains("50%"))));
        Assert.Equal(["a_b"], NativeNames(context, static q => q.Where(static g => g.Name.StartsWith("a_"))));
        Assert.Equal(["back\\slash"], NativeNames(context, static q => q.Where(static g => g.Name.EndsWith("\\slash"))));

        // Same calendar day, different time of day: a DATE mapping would match all five rows.
        Assert.Equal(["a_b", "axb", "back\\slash"], NativeNames(context, q => q.Where(g => g.CreatedAt >= noon)));
    }

    // ------------------------------------------------------------------ LE-27: schema generation

    [Fact]
    public void GenerateDataVoCreateStatements_EmitsVectorsCompositeKeysConstraintsAndIndexes()
    {
        using var context = CreateCatalogContext(InMemory(NewInMemoryDataSource()));

        var statements = context.GenerateDataVoCreateStatements();
        string script = string.Join("\n", statements);

        Assert.Contains("Embedding VECTOR(3) NOT NULL", script, StringComparison.Ordinal);
        Assert.Contains("Features VECTOR(4)", script, StringComparison.Ordinal);
        Assert.Contains("ProjectId INT NOT NULL", script, StringComparison.Ordinal);
        Assert.Contains("PRIMARY KEY (ProjectId, UserId)", script, StringComparison.Ordinal);
        Assert.DoesNotContain("ProjectId INT NOT NULL PRIMARY KEY", script, StringComparison.Ordinal);
        Assert.Contains("Email VARCHAR(80) NOT NULL UNIQUE", script, StringComparison.Ordinal);
        Assert.Contains("Nickname VARCHAR(40),", script, StringComparison.Ordinal);
        Assert.Contains("CREATE INDEX IX_Memberships_Role ON Memberships (Role);", statements);
    }

    [Fact]
    public void EnsureCreated_VectorAndCompositeKeyModel_SupportsCrudAndVectorQueries()
    {
        var connection = InMemory(NewInMemoryDataSource());

        using (var writer = CreateCatalogContext(connection))
        {
            Assert.True(writer.Database.EnsureCreated());
            writer.Add(new Doc { Id = 1, Title = "x-axis", Embedding = [1f, 0f, 0f] });
            writer.Add(new Doc { Id = 2, Title = "y-axis", Embedding = [0f, 1f, 0f] });
            writer.Add(new Clip { Id = 1, Features = new ReadOnlyMemory<float>([1f, 2f, 3f, 4f]) });
            writer.Add(new Membership { ProjectId = 1, UserId = 1, Email = "a@x", Role = "owner" });
            writer.Add(new Membership { ProjectId = 1, UserId = 2, Email = "b@x", Role = "member" });
            writer.SaveChanges();

            writer.Memberships.Single(static m => m.UserId == 2).Role = "admin";
            writer.Remove(writer.Memberships.Single(static m => m.UserId == 1));
            writer.SaveChanges();
        }

        using var context = CreateCatalogContext(connection, native: true);
        float[] query = [0.9f, 0.1f, 0f];

        var byCosine = context.QueryFromDataVo<Doc>(q => q
            .OrderBy(d => DataVoVectorDbFunctions.CosineDistance(EF.Functions, d.Embedding, query))
            .Take(1));
        var byL2 = context.QueryFromDataVo<Doc>(q => q
            .Where(d => DataVoVectorDbFunctions.L2Distance(EF.Functions, d.Embedding, query) < 0.5));

        Assert.Equal("x-axis", Assert.Single(byCosine).Title);
        Assert.Equal([1f, 0f, 0f], Assert.Single(byL2).Embedding);
        Assert.Equal([1f, 2f, 3f, 4f], Assert.Single(context.QueryFromDataVo<Clip>()).Features.ToArray());

        var membership = Assert.Single(context.QueryFromDataVo<Membership>());
        Assert.Equal((1, 2, "admin"), (membership.ProjectId, membership.UserId, membership.Role));

        context.Database.EnsureCreated();
        var duplicate = Assert.Throws<DataVoEfException>(() =>
            context.ExecuteSqlOnDataVo("INSERT INTO Memberships (ProjectId, UserId, Email, Role) VALUES (1, 3, 'b@x', 'r');"));
        Assert.Equal(DataVoErrorCodes.UniqueViolation, duplicate.ErrorCode);
    }

    [Fact]
    public void GenerateDataVoCreateStatements_RejectsUnsupportedShapesWithClearErrors()
    {
        using var missingDimension = new BadVectorContext(BuildOptions<BadVectorContext>(InMemory(NewInMemoryDataSource())));
        var vectorError = Assert.Throws<NotSupportedException>(() => missingDimension.GenerateDataVoCreateStatements());
        Assert.Contains("HasDataVoVectorDimensions", vectorError.Message, StringComparison.Ordinal);

        using var compositeUnique = new CompositeUniqueContext(BuildOptions<CompositeUniqueContext>(InMemory(NewInMemoryDataSource())));
        var uniqueError = Assert.Throws<NotSupportedException>(() => compositeUnique.GenerateDataVoCreateStatements());
        Assert.Contains("single columns only", uniqueError.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void VectorDbFunctions_ClientSideMessagesNameDataVoOperators()
    {
        var cosine = Assert.Throws<NotSupportedException>(() => EF.Functions.CosineDistance([1f], [1f]));
        var l2 = Assert.Throws<NotSupportedException>(() => EF.Functions.L2Distance([1f], [1f]));

        Assert.Contains("<=>", cosine.Message, StringComparison.Ordinal);
        Assert.Contains("<->", l2.Message, StringComparison.Ordinal);
        Assert.Contains("DataVoVectorDbFunctions.L2Distance", DataVoDbContext.GetGuardedQueryCapabilities().SupportedOperators);
    }

    // ------------------------------------------------------------------ LE-28: options and translation parity

    [Fact]
    public void UseDataVo_DoesNotMutateOptionsAlreadyBuilt()
    {
        var builder = new DbContextOptionsBuilder<GadgetContext>()
            .UseInMemoryDatabase($"ef_immutable_{Guid.NewGuid():N}")
            .UseDataVo("StorageMode=InMemory;DataSource=first");
        DbContextOptions<GadgetContext> first = builder.Options;

        builder.UseDataVo("StorageMode=InMemory;DataSource=second", static o => o
            .EnableNativeQueryTranslationPreview()
            .UseLsmStorage()
            .WithDataSource("typed"));
        DbContextOptions<GadgetContext> second = builder.Options;

        var firstSnapshot = first.GetDataVoBootstrapOptions()!;
        Assert.Equal("StorageMode=InMemory;DataSource=first", firstSnapshot.EffectiveConnectionString);
        Assert.Null(firstSnapshot.StorageMode);
        Assert.False(firstSnapshot.NativeQueryTranslationPreviewEnabled);

        var secondSnapshot = second.GetDataVoBootstrapOptions()!;
        Assert.Equal("StorageMode=Lsm;DataSource=typed", secondSnapshot.EffectiveConnectionString);
        Assert.True(secondSnapshot.NativeQueryTranslationPreviewEnabled);
    }

    [Fact]
    public void NativeTranslation_NegationConstantsAndCoalesce_RunNatively()
    {
        var connection = InMemory(NewInMemoryDataSource());
        Gadget[] seed =
        [
            WithFlag(NewGadget(1, "young-yes", age: 5), true),
            WithFlag(NewGadget(2, "young-null", age: 8), null),
            WithFlag(NewGadget(3, "old-no", age: 30), false),
            WithFlag(NewGadget(4, "old-yes", age: 40), true),
        ];

        using (var writer = CreateGadgetContext(connection))
        {
            writer.AddRange(seed);
            writer.SaveChanges();
        }

        using var context = CreateGadgetContext(connection, native: true);
        bool includeAll = true;

        AssertNativeMatchesLinq(context, seed, static q => q.Where(static g => !(g.Age > 10)));
        AssertNativeMatchesLinq(context, seed, static q => q.Where(static g => !(g.Age > 10 && g.Name.StartsWith("young-y"))));
        AssertNativeMatchesLinq(context, seed, static q => q.Where(static g => true));
        AssertNativeMatchesLinq(context, seed, static q => q.Where(static g => false));
        AssertNativeMatchesLinq(context, seed, q => q.Where(g => includeAll));
        AssertNativeMatchesLinq(context, seed, static q => q.Where(static g => g.Flag ?? false));
        AssertNativeMatchesLinq(context, seed, static q => q.Where(static g => g.Flag ?? true));
        AssertNativeMatchesLinq(context, seed, static q => q.Where(static g => g.Name.Contains("old") == false));

        Assert.Equal(2, context.CountFromDataVo<Gadget>(static g => g.Flag ?? false));
        Assert.True(context.AnyFromDataVo<Gadget>(static g => !(g.Age > 35)));
        Assert.Equal("old-yes", context.FirstOrDefaultFromDataVo<Gadget>(static g => g.Age > 35)!.Name);
    }

    [Fact]
    public void NativeTranslation_UnmappedMember_IsReportedAsFallback()
    {
        var connection = InMemory(NewInMemoryDataSource());
        using (var writer = CreateGadgetContext(connection))
        {
            writer.Add(NewGadget(1, "a"));
            writer.SaveChanges();
        }

        using var context = CreateGadgetContext(connection, native: true);

        // Unmapped members are not native-translatable: the analyzer must not promise native execution.
        Assert.True(context.ExplainQueryFromDataVo<Gadget>(static q => q.Where(static g => g.Transient == null)).WillUseGuardedFallback);
        Assert.True(context.ExplainQueryFromDataVo<Gadget>(static q => q.OrderBy(static g => g.Transient)).WillUseGuardedFallback);
        Assert.True(context.ExplainProjectFromDataVo<Gadget, string?>(static g => g.Transient).WillUseGuardedFallback);
        Assert.True(context.ExplainProjectFromDataVo<Gadget, string>(static g => g.Name).WillUseNativeTranslationPreview);
    }

    [Fact]
    public void NativeTranslation_TrackingQuery_ReturnsTrackedEntitiesThroughMirror()
    {
        var connection = InMemory(NewInMemoryDataSource());
        using (var writer = CreateGadgetContext(connection))
        {
            writer.Add(NewGadget(1, "tracked"));
            writer.SaveChanges();
        }

        using var context = CreateGadgetContext(connection, native: true);

        var noTracking = context.ExplainQueryFromDataVo<Gadget>(static q => q.Where(static g => g.Age > 0));
        var tracking = context.ExplainQueryFromDataVo<Gadget>(static q => q.Where(static g => g.Age > 0), asNoTracking: false);
        Assert.True(noTracking.WillUseNativeTranslationPreview);
        Assert.True(tracking.WillUseGuardedFallback);
        Assert.Contains(tracking.FallbackReasons, static reason => reason.Contains("asNoTracking", StringComparison.Ordinal));

        Gadget untracked = Assert.Single(context.QueryFromDataVo<Gadget>(static q => q.Where(static g => g.Age > 0)));
        Assert.Equal(EntityState.Detached, context.Entry(untracked).State);

        Gadget trackedGadget = Assert.Single(context.QueryFromDataVo<Gadget>(static q => q.Where(static g => g.Age > 0), asNoTracking: false));
        Assert.Equal(EntityState.Unchanged, context.Entry(trackedGadget).State);

        trackedGadget.Name = "renamed";
        context.SaveChanges();
        Assert.Equal("renamed", context.FirstOrDefaultFromDataVo<Gadget>()!.Name);
    }

    [Fact]
    public async Task NativeTranslation_HonorsCancellationToken()
    {
        var connection = InMemory(NewInMemoryDataSource());
        using (var writer = CreateGadgetContext(connection))
        {
            writer.Add(NewGadget(1, "a"));
            writer.SaveChanges();
        }

        using var context = CreateGadgetContext(connection, native: true);
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => context.QueryFromDataVoAsync<Gadget>(cancellationToken: cancelled.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => context.CountFromDataVoAsync<Gadget>(cancellationToken: cancelled.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            context.ProjectFromDataVoAsync<Gadget, string>(static g => g.Name, cancellationToken: cancelled.Token));

        Assert.Single(await context.QueryFromDataVoAsync<Gadget>(cancellationToken: CancellationToken.None));
    }

    // ------------------------------------------------------------------ helpers

    private static void AssertNativeMatchesLinq(
        GadgetContext context,
        IEnumerable<Gadget> seed,
        Func<IQueryable<Gadget>, IQueryable<Gadget>> shape)
    {
        var diagnostics = context.ExplainQueryFromDataVo(shape);
        Assert.True(diagnostics.WillUseNativeTranslationPreview, diagnostics.Summary);

        var expected = shape(seed.AsQueryable()).Select(static g => g.Id).OrderBy(static id => id).ToList();
        var actual = context.QueryFromDataVo(shape).Select(static g => g.Id).OrderBy(static id => id).ToList();
        Assert.Equal(expected, actual);
    }

    private static List<string> NativeNames(GadgetContext context, Func<IQueryable<Gadget>, IQueryable<Gadget>> shape)
    {
        Assert.True(context.ExplainQueryFromDataVo(shape).WillUseNativeTranslationPreview);
        return context.QueryFromDataVo(shape).OrderBy(static g => g.Id).Select(static g => g.Name).ToList();
    }

    private static string NewInMemoryDataSource() => $"ef_mem_{Guid.NewGuid():N}";

    private static string InMemory(string dataSource) => $"StorageMode=InMemory;DataSource={dataSource}";

    private static string Lsm(string directory) => $"StorageMode=Lsm;DataSource={directory}";

    private static Gadget NewGadget(int id, string name, int age = 1) => new()
    {
        Id = id,
        Name = name,
        Age = age,
        CreatedAt = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc),
        SeenAt = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero),
        Day = new DateOnly(2026, 1, 1),
        Shade = Shade.Narrow,
        Grade = 'A',
    };

    private static Gadget WithFlag(Gadget gadget, bool? flag)
    {
        gadget.Flag = flag;
        return gadget;
    }

    private static DbContextOptions<TContext> BuildOptions<TContext>(string connectionString, bool native = false)
        where TContext : DbContext
    {
        return new DbContextOptionsBuilder<TContext>()
            .UseInMemoryDatabase($"ef_phase4b_{Guid.NewGuid():N}")
            .UseDataVo(connectionString, o =>
            {
                if (native)
                {
                    o.EnableNativeQueryTranslationPreview();
                }
            })
            .Options;
    }

    private static GadgetContext CreateGadgetContext(string connectionString, bool native = false)
        => new(BuildOptions<GadgetContext>(connectionString, native));

    private static CatalogContext CreateCatalogContext(string connectionString, bool native = false)
        => new(BuildOptions<CatalogContext>(connectionString, native));

    private sealed class GadgetContext(DbContextOptions<GadgetContext> options) : DataVoDbContext(options)
    {
        public DbSet<Gadget> Gadgets => Set<Gadget>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Gadget>(entity =>
            {
                entity.ToTable("Gadgets");
                entity.HasKey(static g => g.Id);
                entity.Property(static g => g.Name).HasMaxLength(80);
                entity.HasIndex(static g => g.Name).IsUnique();
            });
        }
    }

    private sealed class CatalogContext(DbContextOptions<CatalogContext> options) : DataVoDbContext(options)
    {
        public DbSet<Membership> Memberships => Set<Membership>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Doc>(entity =>
            {
                entity.ToTable("Docs");
                entity.HasKey(static d => d.Id);
                entity.Property(static d => d.Title).HasMaxLength(40);
                entity.Property(static d => d.Embedding).HasDataVoVectorDimensions(3);
            });

            modelBuilder.Entity<Clip>(entity =>
            {
                entity.ToTable("Clips");
                entity.HasKey(static c => c.Id);
                entity.Property(static c => c.Features)
                    .HasConversion(static v => VectorBytes.ToBytes(v), static b => VectorBytes.FromBytes(b));
            });

            modelBuilder.Entity<Membership>(entity =>
            {
                entity.ToTable("Memberships");
                entity.HasKey(static m => new { m.ProjectId, m.UserId });
                entity.Property(static m => m.Email).HasMaxLength(80);
                entity.Property(static m => m.Nickname).HasMaxLength(40);
                entity.Property(static m => m.Role).HasMaxLength(20);
                entity.HasIndex(static m => m.Email).IsUnique();
                entity.HasIndex(static m => m.Role);
            });
        }
    }

    private sealed class BadVectorContext(DbContextOptions<BadVectorContext> options) : DataVoDbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Doc>(entity =>
            {
                entity.ToTable("Docs");
                entity.HasKey(static d => d.Id);
            });
        }
    }

    private sealed class CompositeUniqueContext(DbContextOptions<CompositeUniqueContext> options) : DataVoDbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Membership>(entity =>
            {
                entity.ToTable("Memberships");
                entity.HasKey(static m => new { m.ProjectId, m.UserId });
                entity.HasIndex(static m => new { m.Email, m.Role }).IsUnique();
            });
        }
    }

    // The EF host stores the vector as bytes; the DataVo bridge sees the CLR ReadOnlyMemory<float>.
    private static class VectorBytes
    {
        public static byte[] ToBytes(ReadOnlyMemory<float> value)
            => System.Runtime.InteropServices.MemoryMarshal.AsBytes(value.Span).ToArray();

        public static ReadOnlyMemory<float> FromBytes(byte[] value)
            => System.Runtime.InteropServices.MemoryMarshal.Cast<byte, float>(value).ToArray();
    }

    private enum Shade : long
    {
        Narrow = 1,
        Wide = 5_000_000_000L,
    }

    private sealed class Gadget
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public bool? Flag { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTimeOffset SeenAt { get; set; }
        public TimeOnly Alarm { get; set; }
        public DateOnly Day { get; set; }
        public Shade Shade { get; set; }
        public char Grade { get; set; }

        [NotMapped]
        public string? Transient { get; set; }
    }

    private sealed class Doc
    {
        public int Id { get; set; }
        public string Title { get; set; } = string.Empty;
        public float[] Embedding { get; set; } = [];
    }

    private sealed class Clip
    {
        public int Id { get; set; }

        [VectorDimensions(4)]
        public ReadOnlyMemory<float> Features { get; set; }
    }

    private sealed class Membership
    {
        public int ProjectId { get; set; }
        public int UserId { get; set; }
        public string Email { get; set; } = string.Empty;
        public string? Nickname { get; set; }
        public string Role { get; set; } = string.Empty;
    }
}
