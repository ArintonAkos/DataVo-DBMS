using System.Reflection;
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.VectorData;

namespace DataVo.VectorData.Tests;

/// <summary>
/// Regression and coverage tests for the audit items BC-23 and LE-29 to LE-33: text search over pre-embedded vectors,
/// rejected full-text indexes, dynamic Guid key generation, AOT-safe filter evaluation, escaped LIKE, cancellation,
/// <c>GetService</c>, DI registrations, session-pool eviction, and the mapping paths the original suite left out.
/// </summary>
public sealed class DataVoVectorStoreCoverageTests : IDisposable
{
    private static readonly DateTime Jan = new(2026, 1, 15, 8, 0, 0, DateTimeKind.Utc);
    private readonly List<string> _directories = [];

    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    public void Dispose()
    {
        foreach (string directory in _directories.Where(Directory.Exists))
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    // ---------------------------------------------------------------- LE-29

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task TextSearch_OnPreEmbeddedVector_UsesStoreGenerator(StorageMode mode)
    {
        var generator = new TestEmbeddingGenerator();
        using DataVoVectorStore store = Open(mode, new DataVoVectorStoreOptions { EmbeddingGenerator = generator });
        var docs = new DataVoVectorStoreCollection<int, PreEmbeddedDoc>(store, "pre");
        await docs.EnsureCollectionExistsAsync();

        string[] texts = ["abc", "fgh", "aab"];
        GeneratedEmbeddings<Embedding<float>> vectors = await generator.GenerateAsync(texts);
        for (int i = 0; i < texts.Length; i++)
        {
            await docs.UpsertAsync(new PreEmbeddedDoc { Id = i + 1, Text = texts[i], Vector = vectors[i].Vector });
        }

        VectorSearchResult<PreEmbeddedDoc> hit = await docs.SearchAsync("fgh", top: 1).SingleAsync();

        Assert.Equal(2, hit.Record.Id);
    }

    [Fact]
    public async Task TextSearch_WithoutGenerator_StillThrowsNotSupported()
    {
        using DataVoVectorStore store = Open(StorageMode.InMemory);
        var docs = new DataVoVectorStoreCollection<int, PreEmbeddedDoc>(store, "pre");
        await docs.EnsureCollectionExistsAsync();

        await Assert.ThrowsAsync<NotSupportedException>(async () => await docs.SearchAsync("abc", top: 1).ToListAsync());
    }

    // ---------------------------------------------------------------- LE-30

    [Fact]
    public void FullTextIndexedProperty_IsRejected()
    {
        using DataVoVectorStore store = Open(StorageMode.InMemory);
        NotSupportedException attribute = Assert.Throws<NotSupportedException>(() => new DataVoVectorStoreCollection<int, FullTextDoc>(store, "ft"));
        Assert.Contains("IsFullTextIndexed", attribute.Message, StringComparison.Ordinal);

        var definition = new VectorStoreCollectionDefinition
        {
            Properties =
            [
                new VectorStoreKeyProperty("Id", typeof(int)),
                new VectorStoreDataProperty("Body", typeof(string)) { IsFullTextIndexed = true },
                new VectorStoreVectorProperty("Vector", typeof(ReadOnlyMemory<float>), 2),
            ],
        };
        Assert.Throws<NotSupportedException>(() => store.GetDynamicCollection("ft", definition));
    }

    [Fact]
    public void UnsupportedDataType_IsRejected()
    {
        using DataVoVectorStore store = Open(StorageMode.InMemory);
        Assert.Throws<NotSupportedException>(() => new DataVoVectorStoreCollection<int, UnsupportedTypeDoc>(store, "bad"));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task DynamicUpsert_GeneratesGuidKey_WhenNull(StorageMode mode)
    {
        using DataVoVectorStore store = Open(mode);
        var definition = new VectorStoreCollectionDefinition
        {
            Properties =
            [
                new VectorStoreKeyProperty("Id", typeof(Guid)),
                new VectorStoreDataProperty("Name", typeof(string)),
                new VectorStoreVectorProperty("Vector", typeof(ReadOnlyMemory<float>), 2),
            ],
        };
        VectorStoreCollection<object, Dictionary<string, object?>> items = store.GetDynamicCollection("items", definition);
        await items.EnsureCollectionExistsAsync();

        var record = new Dictionary<string, object?> { ["Id"] = null, ["Name"] = "auto", ["Vector"] = new ReadOnlyMemory<float>([1, 0]) };
        await items.UpsertAsync(record);
        var missingKey = new Dictionary<string, object?> { ["Name"] = "absent", ["Vector"] = new ReadOnlyMemory<float>([0, 1]) };
        await items.UpsertAsync(missingKey);

        Guid id = Assert.IsType<Guid>(record["Id"]);
        Assert.NotEqual(Guid.Empty, id);
        Assert.NotEqual(Guid.Empty, Assert.IsType<Guid>(missingKey["Id"]));
        Assert.Equal("auto", (await items.GetAsync(id))!["Name"]);
    }

    // ---------------------------------------------------------------- LE-31

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task Filters_EvaluateConstructorsArithmeticAndConditionals(StorageMode mode)
    {
        using DataVoVectorStore store = Open(mode);
        DataVoVectorStoreCollection<int, Doc> docs = await SeedDocs(store);
        List<Doc> all = await docs.GetAsync(doc => true, top: 100).ToListAsync();
        int min = 5;
        int negative = -3;
        bool high = true;
        TimeSpan hour = TimeSpan.FromHours(1);

        await AssertSame(docs, all, doc => doc.UpdatedAt > new DateTime(2026, 1, 20, 0, 0, 0, DateTimeKind.Utc));
        await AssertSame(docs, all, doc => doc.Rank > min + 1);
        await AssertSame(docs, all, doc => doc.Rank > min * 2 - 1);
        await AssertSame(docs, all, doc => doc.Rank >= -negative);
        await AssertSame(docs, all, doc => doc.Rank == (high ? 12 : 1));
        await AssertSame(docs, all, doc => doc.Rank > min % 3);
        await AssertSame(docs, all, doc => doc.Rank < (min | 8));
        await AssertSame(docs, all, doc => doc.UpdatedAt >= Jan.AddDays(3) + hour);
        await AssertSame(docs, all, doc => doc.UpdatedAt < new DateTimeOffset(2026, 1, 20, 0, 0, 0, TimeSpan.Zero).UtcDateTime);
        await AssertSame(docs, all, doc => doc.Published == !high);
        await AssertSame(docs, all, doc => doc.Category == (high ? "guide" : "reference"));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task Filters_EvaluateGuidDateOnlyAndDecimalConstructors(StorageMode mode)
    {
        using DataVoVectorStore store = Open(mode);
        var docs = new DataVoVectorStoreCollection<int, TypedDataDoc>(store, "typed");
        await docs.EnsureCollectionExistsAsync();
        Guid reference = new("6f9619ff-8b86-d011-b42d-00c04fc964ff");
        await docs.UpsertAsync([
            NewTyped(1, new DateOnly(2026, 3, 1), reference, 10.5m),
            NewTyped(2, new DateOnly(2026, 4, 1), Guid.NewGuid(), 99.25m),
        ]);

        Assert.Equal([1], await Ids(docs, doc => doc.Ref == new Guid("6f9619ff-8b86-d011-b42d-00c04fc964ff")));
        Assert.Equal([2], await Ids(docs, doc => doc.Day > new DateOnly(2026, 3, 15)));
        Assert.Equal([2], await Ids(docs, doc => doc.Price > 10.5m * 2));
        Assert.Equal([1, 2], await Ids(docs, doc => doc.Day >= new DateOnly()));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task StringFilters_EscapeLikeWildcards(StorageMode mode)
    {
        using DataVoVectorStore store = Open(mode);
        var docs = new DataVoVectorStoreCollection<int, Doc>(store, "likes");
        await docs.EnsureCollectionExistsAsync();
        string[] paths = ["a%b.md", "a_b.md", "axb.md", @"a\b.md", "50%_off"];
        for (int i = 0; i < paths.Length; i++)
        {
            Doc doc = NewDoc(i + 1, "guide", [1, 0, 0, 0]);
            doc.FilePath = paths[i];
            await docs.UpsertAsync(doc);
        }

        Assert.Equal([1], await Ids(docs, doc => doc.FilePath.StartsWith("a%")));
        Assert.Equal([2], await Ids(docs, doc => doc.FilePath.StartsWith("a_")));
        Assert.Equal([2], await Ids(docs, doc => doc.FilePath.EndsWith("_b.md")));
        Assert.Equal([1, 5], await Ids(docs, doc => doc.FilePath.Contains('%'.ToString())));
        Assert.Equal([4], await Ids(docs, doc => doc.FilePath.Contains(@"\")));
        Assert.Equal([5], await Ids(docs, doc => doc.FilePath.EndsWith("%_off")));
        Assert.Equal([1, 2, 3, 4], await Ids(docs, doc => doc.FilePath.EndsWith("b.md")));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task InFilter_HandlesNullAndEmptyCollections(StorageMode mode)
    {
        using DataVoVectorStore store = Open(mode);
        var docs = new DataVoVectorStoreCollection<int, Doc>(store, "notes");
        await docs.EnsureCollectionExistsAsync();
        string?[] notes = [null, "x", "y"];
        for (int i = 0; i < notes.Length; i++)
        {
            Doc doc = NewDoc(i + 1, "guide", [1, 0, 0, 0]);
            doc.Note = notes[i];
            await docs.UpsertAsync(doc);
        }

        string?[] onlyNull = [null];
        string?[] nullOrX = [null, "x"];
        string[] empty = [];
        List<string?> list = ["y"];

        Assert.Equal([1], await Ids(docs, doc => onlyNull.Contains(doc.Note)));
        Assert.Equal([1, 2], await Ids(docs, doc => nullOrX.Contains(doc.Note)));
        Assert.Empty(await Ids(docs, doc => empty.Contains(doc.Note!)));
        Assert.Equal([2, 3], await Ids(docs, doc => !onlyNull.Contains(doc.Note)));
        Assert.Equal([3], await Ids(docs, doc => list.Contains(doc.Note)));
    }

    // ---------------------------------------------------------------- LE-32

    [Fact]
    public async Task CanceledToken_StopsEveryOperation()
    {
        using DataVoVectorStore store = Open(StorageMode.InMemory);
        var docs = new DataVoVectorStoreCollection<int, Doc>(store, "docs");
        await docs.EnsureCollectionExistsAsync();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        CancellationToken token = cancellation.Token;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => docs.CollectionExistsAsync(token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => docs.EnsureCollectionExistsAsync(token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => docs.EnsureCollectionDeletedAsync(token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => docs.UpsertAsync(NewDoc(1, "guide", [1, 0, 0, 0]), token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => docs.UpsertAsync([NewDoc(2, "guide", [1, 0, 0, 0])], token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => docs.DeleteAsync(1, token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => docs.DeleteAsync([1], token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => docs.GetAsync(1, cancellationToken: token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await docs.GetAsync([1], cancellationToken: token).ToListAsync());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.CollectionExistsAsync("docs", token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.EnsureCollectionDeletedAsync("docs", token));

        // Nothing ran: the collection still exists and is empty.
        Assert.True(await docs.CollectionExistsAsync());
        Assert.Null(await docs.GetAsync(1));
    }

    [Fact]
    public async Task GetService_ExposesContextAndEmbeddingGenerator()
    {
        var generator = new TestEmbeddingGenerator();
        using DataVoVectorStore store = Open(StorageMode.InMemory, new DataVoVectorStoreOptions { EmbeddingGenerator = generator });
        var docs = new DataVoVectorStoreCollection<int, PreEmbeddedDoc>(store, "pre");
        var dynamic = store.GetDynamicCollection("dyn", new VectorStoreCollectionDefinition
        {
            Properties =
            [
                new VectorStoreKeyProperty("Id", typeof(int)),
                new VectorStoreVectorProperty("Vector", typeof(ReadOnlyMemory<float>), TestEmbeddingGenerator.Dimensions),
            ],
        });

        Assert.Same(store.Context, docs.GetService(typeof(DataVoContext)));
        Assert.Same(store.Context, dynamic.GetService(typeof(DataVoContext)));
        Assert.Same(generator, docs.GetService(typeof(IEmbeddingGenerator)));
        Assert.Same(generator, docs.GetService(typeof(IEmbeddingGenerator<string, Embedding<float>>)));
        Assert.Same(generator, dynamic.GetService(typeof(IEmbeddingGenerator)));
        Assert.Same(generator, store.GetService(typeof(IEmbeddingGenerator)));
        Assert.Null(docs.GetService(typeof(IEmbeddingGenerator), serviceKey: "other"));
        Assert.Null(docs.GetService(typeof(Uri)));

        using DataVoVectorStore plain = Open(StorageMode.InMemory);
        Assert.Null(new DataVoVectorStoreCollection<int, Doc>(plain, "docs").GetService(typeof(IEmbeddingGenerator)));
        await Task.CompletedTask;
    }

    [Fact]
    public async Task DependencyInjection_KeyedAndDynamicCollections()
    {
        var services = new ServiceCollection();
        services.AddDataVoVectorStore(new DataVoConfig { StorageMode = StorageMode.InMemory });
        services.AddKeyedDataVoVectorStore("cold", new DataVoConfig { StorageMode = StorageMode.InMemory });
        services.AddKeyedDataVoCollection<int, Doc>("hot-docs", "docs");
        services.AddKeyedDataVoCollection<int, Doc>("cold-docs", "docs", storeKey: "cold");
        var definition = new VectorStoreCollectionDefinition
        {
            Properties =
            [
                new VectorStoreKeyProperty("Id", typeof(int)),
                new VectorStoreVectorProperty("Vector", typeof(ReadOnlyMemory<float>), 2),
            ],
        };
        services.AddDataVoDynamicCollection("points", definition);
        services.AddKeyedDataVoDynamicCollection("cold-points", "points", definition, storeKey: "cold");
        await using ServiceProvider provider = services.BuildServiceProvider();

        VectorStoreCollection<int, Doc> hot = provider.GetRequiredKeyedService<VectorStoreCollection<int, Doc>>("hot-docs");
        VectorStoreCollection<int, Doc> cold = provider.GetRequiredKeyedService<VectorStoreCollection<int, Doc>>("cold-docs");
        Assert.Same(hot, provider.GetRequiredKeyedService<IVectorSearchable<Doc>>("hot-docs"));
        Assert.Same(provider.GetRequiredService<DataVoVectorStore>().Context, hot.GetService(typeof(DataVoContext)));
        Assert.Same(provider.GetRequiredKeyedService<DataVoVectorStore>("cold").Context, cold.GetService(typeof(DataVoContext)));

        await hot.EnsureCollectionExistsAsync();
        await hot.UpsertAsync(NewDoc(1, "guide", [1, 0, 0, 0]));
        Assert.False(await cold.CollectionExistsAsync(), "the keyed collection lives in the keyed store");

        VectorStoreCollection<object, Dictionary<string, object?>> points = provider.GetRequiredService<VectorStoreCollection<object, Dictionary<string, object?>>>();
        Assert.Same(points, provider.GetRequiredService<IVectorSearchable<Dictionary<string, object?>>>());
        await points.EnsureCollectionExistsAsync();
        await points.UpsertAsync(new Dictionary<string, object?> { ["Id"] = 4, ["Vector"] = new ReadOnlyMemory<float>([0, 1]) });
        Assert.Equal(4, (await points.SearchAsync(new float[] { 0, 1 }, top: 1).SingleAsync()).Record["Id"]);

        var coldPoints = provider.GetRequiredKeyedService<VectorStoreCollection<object, Dictionary<string, object?>>>("cold-points");
        Assert.False(await coldPoints.CollectionExistsAsync());
    }

    // ---------------------------------------------------------------- BC-23 / LE-33

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task ExplicitNonAutoGeneratedGuidKey_KeepsEmptyGuid(StorageMode mode)
    {
        using DataVoVectorStore store = Open(mode);
        var manual = new DataVoVectorStoreCollection<Guid, ManualGuidDoc>(store, "manual");
        await manual.EnsureCollectionExistsAsync();
        var empty = new ManualGuidDoc { Id = Guid.Empty, Name = "zero", Vector = new float[] { 1, 0 } };
        await manual.UpsertAsync(empty);

        Assert.Equal(Guid.Empty, empty.Id);
        Assert.Equal("zero", (await manual.GetAsync(Guid.Empty))!.Name);

        var generated = new DataVoVectorStoreCollection<Guid, DefaultGuidDoc>(store, "generated");
        await generated.EnsureCollectionExistsAsync();
        var record = new DefaultGuidDoc { Vector = new float[] { 1, 0 } };
        await generated.UpsertAsync(record);
        Assert.NotEqual(Guid.Empty, record.Id);
    }

    [Fact]
    public void KeyAttribute_StillCarriesTheNullableAutoGeneratedFlag()
    {
        // Canary for the abstractions package: when this internal property disappears, the model falls back to the
        // attribute's recorded named argument (covered by ExplicitNonAutoGeneratedGuidKey_KeepsEmptyGuid).
        PropertyInfo? property = typeof(VectorStoreKeyAttribute).GetProperty(
            DataVoCollectionModel.AutoGeneratedNullableProperty, BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(property);
        Assert.Equal(typeof(bool?), property.PropertyType);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task MultiVectorRecord_WithStorageNames_SearchesEachVector(StorageMode mode)
    {
        using DataVoVectorStore store = Open(mode);
        var docs = new DataVoVectorStoreCollection<int, MultiVectorDoc>(store, "multi");
        await docs.EnsureCollectionExistsAsync();
        await docs.UpsertAsync([
            new MultiVectorDoc { Id = 1, Title = "one", TitleVector = new float[] { 1, 0, 0 }, BodyVector = [0, 5] },
            new MultiVectorDoc { Id = 2, Title = "two", TitleVector = new float[] { 0, 1, 0 }, BodyVector = [5, 0] },
        ]);

        Assert.Contains("title_vec", store.Context.Engine.Catalog.GetTableColumns("multi", store.Options.DatabaseName).Select(c => c.Name));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await docs.SearchAsync(new float[] { 1, 0, 0 }, top: 1).ToListAsync());

        var byTitle = new VectorSearchOptions<MultiVectorDoc> { VectorProperty = doc => doc.TitleVector, IncludeVectors = true };
        VectorSearchResult<MultiVectorDoc> title = await docs.SearchAsync(new float[] { 0, 1, 0 }, top: 1, byTitle).SingleAsync();
        Assert.Equal(2, title.Record.Id);
        Assert.Equal([5f, 0f], title.Record.BodyVector!);

        var byBody = new VectorSearchOptions<MultiVectorDoc> { VectorProperty = doc => doc.BodyVector };
        Assert.Equal(1, (await docs.SearchAsync(new float[] { 0, 4 }, top: 1, byBody).SingleAsync()).Record.Id);
        Assert.Equal([1], (await docs.GetAsync(doc => doc.Title == "one", top: 5).ToListAsync()).Select(doc => doc.Id));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task TypedDefinition_MapsAttributelessRecord(StorageMode mode)
    {
        using DataVoVectorStore store = Open(mode);
        var definition = new VectorStoreCollectionDefinition
        {
            Properties =
            [
                new VectorStoreKeyProperty(nameof(PlainDoc.Code), typeof(string)) { StorageName = "code_key" },
                new VectorStoreDataProperty(nameof(PlainDoc.Size), typeof(int)) { IsIndexed = true },
                new VectorStoreVectorProperty(nameof(PlainDoc.Embedding), typeof(ReadOnlyMemory<float>), 2) { DistanceFunction = DistanceFunction.EuclideanDistance },
            ],
        };
        var plain = new DataVoVectorStoreCollection<string, PlainDoc>(store, "plain", definition);
        await plain.EnsureCollectionExistsAsync();
        await plain.UpsertAsync([
            new PlainDoc { Code = "a", Size = 1, Embedding = new float[] { 0, 0 } },
            new PlainDoc { Code = "b", Size = 2, Embedding = new float[] { 3, 4 } },
        ]);

        Assert.Equal(2, (await plain.GetAsync("b"))!.Size);
        VectorSearchResult<PlainDoc> hit = await plain.SearchAsync(new float[] { 3, 4 }, top: 1).SingleAsync();
        Assert.Equal("b", hit.Record.Code);
        Assert.Equal(0, hit.Score!.Value, 3);
        Assert.Equal(["a"], (await plain.GetAsync(doc => doc.Size < 2, top: 5).ToListAsync()).Select(doc => doc.Code));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task DynamicObjectKeys_AreStoredAsText(StorageMode mode)
    {
        using DataVoVectorStore store = Open(mode);
        var definition = new VectorStoreCollectionDefinition
        {
            Properties =
            [
                new VectorStoreKeyProperty("Key", typeof(object)),
                new VectorStoreVectorProperty("Vector", typeof(ReadOnlyMemory<float>), 2),
            ],
        };
        VectorStoreCollection<object, Dictionary<string, object?>> items = store.GetDynamicCollection("objkeys", definition);
        await items.EnsureCollectionExistsAsync();
        await items.UpsertAsync([
            new Dictionary<string, object?> { ["Key"] = 5, ["Vector"] = new ReadOnlyMemory<float>([1, 0]) },
            new Dictionary<string, object?> { ["Key"] = "five", ["Vector"] = new ReadOnlyMemory<float>([0, 1]) },
        ]);

        Assert.Equal("5", (await items.GetAsync(5))!["Key"]);
        Assert.Equal("5", (await items.GetAsync("5"))!["Key"]);
        Assert.Equal("five", (await items.GetAsync("five"))!["Key"]);
        await items.DeleteAsync(5);
        Assert.Null(await items.GetAsync("5"));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task DataTypes_RoundTrip(StorageMode mode)
    {
        using DataVoVectorStore store = Open(mode);
        var docs = new DataVoVectorStoreCollection<int, TypedDataDoc>(store, "typed");
        await docs.EnsureCollectionExistsAsync();
        Guid reference = Guid.NewGuid();
        TypedDataDoc original = NewTyped(1, new DateOnly(2026, 2, 28), reference, 1234.5m);
        original.At = new DateTimeOffset(2026, 2, 28, 10, 30, 0, TimeSpan.FromHours(2));
        await docs.UpsertAsync(original);

        TypedDataDoc read = (await docs.GetAsync(1))!;

        Assert.Equal(new DateOnly(2026, 2, 28), read.Day);
        Assert.Equal(original.At, read.At); // the same instant (returned in UTC)
        Assert.Equal(TimeSpan.Zero, read.At.Offset);
        Assert.Equal(1234.5m, read.Price);
        Assert.Equal((short)-12, read.Small);
        Assert.Equal((byte)250, read.Tiny);
        Assert.Equal(0.25f, read.Ratio);
        Assert.Equal(["x", "y"], read.Labels);
        Assert.Equal(reference, read.Ref);
        Assert.Equal([1], await Ids(docs, doc => doc.Small < 0 && doc.Tiny > 200 && doc.Ratio < 0.5f));
    }

    [Fact]
    public void SessionPool_EvictsSurplusSessionsAndStatements()
    {
        using var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        context.Execute("CREATE DATABASE PoolDb");
        var pool = new DataVoSessionPool(context, "PoolDb");

        var leases = Enumerable.Range(0, DataVoSessionPool.MaxIdleSessions + 8).Select(_ => pool.Rent()).ToList();
        foreach (DataVoSessionPool.Lease lease in leases)
        {
            lease.Dispose();
        }

        Assert.Equal(DataVoSessionPool.MaxIdleSessions, pool.IdleCount);
        int unregistered = leases.Count(lease => context.Engine.Sessions.Get(lease.Session) is null);
        Assert.Equal(8, unregistered);

        using DataVoSessionPool.Lease statements = pool.Rent();
        Assert.False(statements.Execute("CREATE TABLE T (Id INT PRIMARY KEY)").IsError);
        for (int i = 0; i <= DataVoSessionPool.MaxCachedStatements + 5; i++)
        {
            Assert.False(statements.Execute($"SELECT Id FROM T WHERE Id = @p{i}", DataVoQueryParameter.FromObject("p" + i, i)).IsError);
            Assert.InRange(statements.CachedStatementCount, 1, DataVoSessionPool.MaxCachedStatements);
        }

        pool.SchemaChanged();
        statements.Activate(int.MaxValue);
        Assert.Equal(0, statements.CachedStatementCount);
    }

    private static async Task AssertSame(
        DataVoVectorStoreCollection<int, Doc> docs,
        List<Doc> all,
        System.Linq.Expressions.Expression<Func<Doc, bool>> filter)
    {
        Func<Doc, bool> predicate = filter.Compile();
        List<int> expected = [.. all.Where(predicate).Select(doc => doc.Id).Order()];
        List<int> actual = [.. (await docs.GetAsync(filter, top: 100).ToListAsync()).Select(doc => doc.Id).Order()];
        Assert.Equal(expected, actual);
    }

    private static async Task<List<int>> Ids<TRecord>(
        DataVoVectorStoreCollection<int, TRecord> docs,
        System.Linq.Expressions.Expression<Func<TRecord, bool>> filter)
        where TRecord : class
    {
        List<TRecord> records = await docs.GetAsync(filter, top: 100).ToListAsync();
        return [.. records.Select(record => (int)typeof(TRecord).GetProperty("Id")!.GetValue(record)!).Order()];
    }

    private static TypedDataDoc NewTyped(int id, DateOnly day, Guid reference, decimal price) => new()
    {
        Id = id,
        Day = day,
        At = new DateTimeOffset(day.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero),
        Price = price,
        Small = -12,
        Tiny = 250,
        Ratio = 0.25f,
        Labels = ["x", "y"],
        Ref = reference,
        Vector = new float[] { 1, 0 },
    };

    private DataVoVectorStore Open(StorageMode mode, DataVoVectorStoreOptions? options = null) =>
        new(new DataVoConfig
        {
            StorageMode = mode,
            DataDirectory = mode == StorageMode.Lsm ? NewDirectory() : null,
            LsmStrictFsync = false,
        }, options);

    private string NewDirectory()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"datavo_vectordata_cov_{Guid.NewGuid():N}");
        _directories.Add(directory);
        return directory;
    }

    /// <summary>Twelve documents: categories alternate guide/reference, ranks 1-12, one day apart, every other published.</summary>
    private static async Task<DataVoVectorStoreCollection<int, Doc>> SeedDocs(DataVoVectorStore store)
    {
        var docs = new DataVoVectorStoreCollection<int, Doc>(store, "docs");
        await docs.EnsureCollectionExistsAsync();
        for (int i = 1; i <= 12; i++)
        {
            Doc doc = NewDoc(i, i % 2 == 0 ? "reference" : "guide", [i, 1, 0, 0]);
            doc.Published = i % 2 == 0;
            await docs.UpsertAsync(doc);
        }

        return docs;
    }

    private static Doc NewDoc(int id, string category, float[] vector) => new()
    {
        Id = id,
        Category = category,
        FilePath = $"docs/{category}/{id}.md",
        UpdatedAt = Jan.AddDays(id),
        Rank = id,
        Published = true,
        Vector = vector,
    };
}
