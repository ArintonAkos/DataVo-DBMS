using DataVo.Core;
using DataVo.Core.StorageEngine.Config;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.VectorData;

namespace DataVo.VectorData.Tests;

/// <summary>
/// <see cref="DataVoVectorStore"/> and its collections against the in-memory and LSM engines: schema mapping, CRUD,
/// filtered and paged retrieval, vector search over HNSW and FLAT indexes in every supported distance function, the
/// vector CLR shapes (<see cref="ReadOnlyMemory{T}"/>, <c>float[]</c>, <see cref="Embedding{T}"/>), embedding
/// generation, dynamic collections, LSM persistence across reopen, and dependency injection.
/// </summary>
public sealed class DataVoVectorStoreTests : IDisposable
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

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task Crud_RoundTripsEveryPropertyShape(StorageMode mode)
    {
        using DataVoVectorStore store = Open(mode);
        var docs = new DataVoVectorStoreCollection<int, Doc>(store, "docs");
        Assert.False(await docs.CollectionExistsAsync());
        await docs.EnsureCollectionExistsAsync();
        await docs.EnsureCollectionExistsAsync(); // idempotent
        Assert.True(await docs.CollectionExistsAsync());

        Doc first = NewDoc(1, "guide", [1, 0, 0, 0]);
        first.Note = null;
        first.Tags = ["intro", "lsm"];
        await docs.UpsertAsync(first);

        Doc? read = await docs.GetAsync(1, new RecordRetrievalOptions { IncludeVectors = true });
        Assert.NotNull(read);
        Assert.Equal("guide", read.Category);
        Assert.Equal("docs/guide/1.md", read.FilePath);
        Assert.Equal(Jan.AddDays(1), read.UpdatedAt);
        Assert.Equal(DateTimeKind.Utc, read.UpdatedAt.Kind);
        Assert.Equal(1, read.Rank);
        Assert.True(read.Published);
        Assert.Null(read.Note);
        Assert.Equal(["intro", "lsm"], read.Tags);
        Assert.Equal([1f, 0f, 0f, 0f], read.Vector.ToArray());

        Doc? withoutVector = await docs.GetAsync(1);
        Assert.NotNull(withoutVector);
        Assert.True(withoutVector.Vector.IsEmpty);

        // Upsert replaces the whole record.
        Doc changed = NewDoc(1, "reference", [0, 1, 0, 0]);
        changed.Note = "edited";
        await docs.UpsertAsync(changed);
        Doc replaced = (await docs.GetAsync(1, new RecordRetrievalOptions { IncludeVectors = true }))!;
        Assert.Equal("reference", replaced.Category);
        Assert.Equal("edited", replaced.Note);
        Assert.Equal([0f, 1f, 0f, 0f], replaced.Vector.ToArray());

        await docs.UpsertAsync([NewDoc(2, "guide", [0, 0, 1, 0]), NewDoc(3, "guide", [0, 0, 0, 1])]);
        List<Doc> many = await docs.GetAsync([1, 2, 3, 99]).ToListAsync();
        Assert.Equal([1, 2, 3], many.Select(doc => doc.Id).Order());

        await docs.DeleteAsync(2);
        await docs.DeleteAsync([3, 42]);
        Assert.Null(await docs.GetAsync(2));
        Assert.Equal([1], (await docs.GetAsync([1, 2, 3]).ToListAsync()).Select(doc => doc.Id));

        await docs.EnsureCollectionDeletedAsync();
        Assert.False(await docs.CollectionExistsAsync());
        await docs.EnsureCollectionDeletedAsync(); // idempotent
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task Search_HnswCosineSimilarity_RanksAndScores(StorageMode mode)
    {
        using DataVoVectorStore store = Open(mode);
        DataVoVectorStoreCollection<int, Doc> docs = await SeedDocs(store);

        List<VectorSearchResult<Doc>> hits = await docs.SearchAsync(new ReadOnlyMemory<float>([1, 0.1f, 0, 0]), top: 3).ToListAsync();

        Assert.Equal(3, hits.Count);
        Assert.Equal(1, hits[0].Record.Id);
        Assert.True(hits[0].Score > hits[1].Score && hits[1].Score >= hits[2].Score, "similarity scores descend");
        Assert.InRange(hits[0].Score!.Value, 0.99, 1.0);
        Assert.True(hits[0].Record.Vector.IsEmpty, "vectors are not returned unless asked");

        // Every search input shape ranks the same.
        float[] query = [1, 0.1f, 0, 0];
        Assert.Equal(Ids(hits), Ids(await docs.SearchAsync(query, top: 3).ToListAsync()));
        Assert.Equal(Ids(hits), Ids(await docs.SearchAsync(new Embedding<float>(query), top: 3).ToListAsync()));

        // Skip, IncludeVectors and a similarity threshold.
        List<VectorSearchResult<Doc>> paged = await docs.SearchAsync(query, top: 2, new VectorSearchOptions<Doc> { Skip = 1, IncludeVectors = true }).ToListAsync();
        Assert.Equal(Ids(hits).Skip(1), Ids(paged));
        Assert.False(paged[0].Record.Vector.IsEmpty);
        List<VectorSearchResult<Doc>> close = await docs.SearchAsync(query, top: 10, new VectorSearchOptions<Doc> { ScoreThreshold = 0.9 }).ToListAsync();
        Assert.All(close, hit => Assert.True(hit.Score >= 0.9));
        Assert.Contains(close, hit => hit.Record.Id == 1);

        await Assert.ThrowsAsync<ArgumentException>(async () => await docs.SearchAsync(new float[] { 1, 0 }, top: 1).ToListAsync());
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task Search_FilteredHybrid_MatchesLinqOverTheSameRecords(StorageMode mode)
    {
        using DataVoVectorStore store = Open(mode);
        DataVoVectorStoreCollection<int, Doc> docs = await SeedDocs(store);
        List<Doc> all = await docs.GetAsync(doc => doc.Id > 0, top: 100, new FilteredRecordRetrievalOptions<Doc> { IncludeVectors = true }).ToListAsync();
        Assert.Equal(12, all.Count);

        DateTime since = Jan.AddDays(4);
        string category = "guide";
        int[] picked = [2, 5, 7];
        string prefix = "docs/ref";
        var filters = new (System.Linq.Expressions.Expression<Func<Doc, bool>> Filter, Func<Doc, bool> Expected)[]
        {
            (doc => doc.Category == category, doc => doc.Category == category),
            (doc => doc.Category == "guide" && doc.UpdatedAt >= since, doc => doc.Category == "guide" && doc.UpdatedAt >= since),
            (doc => doc.Rank < 3 || doc.Rank > 10, doc => doc.Rank < 3 || doc.Rank > 10),
            (doc => !(doc.Rank <= 6), doc => !(doc.Rank <= 6)),
            (doc => doc.Published, doc => doc.Published),
            (doc => !doc.Published && doc.Note != null, doc => !doc.Published && doc.Note != null),
            (doc => doc.Note == null, doc => doc.Note == null),
            (doc => picked.Contains(doc.Id), doc => picked.Contains(doc.Id)),
            (doc => doc.FilePath.StartsWith(prefix), doc => doc.FilePath.StartsWith(prefix, StringComparison.Ordinal)),
            (doc => doc.FilePath.EndsWith("1.md"), doc => doc.FilePath.EndsWith("1.md", StringComparison.Ordinal)),
            (doc => doc.FilePath.Contains("guide/1"), doc => doc.FilePath.Contains("guide/1", StringComparison.Ordinal)),
            (doc => doc.FilePath.StartsWith("docs/guide_"), doc => doc.FilePath.StartsWith("docs/guide_", StringComparison.Ordinal)),
            (doc => 5 > doc.Rank, doc => 5 > doc.Rank),
        };

        float[] query = [0.2f, 1, 0.1f, 0];
        foreach ((System.Linq.Expressions.Expression<Func<Doc, bool>> filter, Func<Doc, bool> expected) in filters)
        {
            List<int> expectedIds = [.. all.Where(expected)
                .OrderByDescending(doc => Cosine(doc.Vector.Span, query)).ThenBy(doc => doc.Id)
                .Take(4).Select(doc => doc.Id)];
            List<VectorSearchResult<Doc>> hits = await docs.SearchAsync(query, top: 4, new VectorSearchOptions<Doc> { Filter = filter }).ToListAsync();
            Assert.True(expectedIds.Order().SequenceEqual(Ids(hits).Order()), $"{filter}: expected [{string.Join(",", expectedIds)}], got [{string.Join(",", Ids(hits))}]");

            List<int> retrieved = [.. (await docs.GetAsync(filter, top: 100).ToListAsync()).Select(doc => doc.Id).Order()];
            Assert.Equal([.. all.Where(expected).Select(doc => doc.Id).Order()], retrieved);
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task GetWithFilter_OrdersSkipsAndTakes(StorageMode mode)
    {
        using DataVoVectorStore store = Open(mode);
        DataVoVectorStoreCollection<int, Doc> docs = await SeedDocs(store);

        List<Doc> page = await docs.GetAsync(
            doc => doc.Rank >= 2,
            top: 3,
            new FilteredRecordRetrievalOptions<Doc>
            {
                OrderBy = order => order.Descending(doc => doc.UpdatedAt).Ascending(doc => doc.Id),
                Skip = 2,
            }).ToListAsync();

        Assert.Equal([10, 9, 8], page.Select(doc => doc.Id));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task UnsupportedFilters_Throw(StorageMode mode)
    {
        using DataVoVectorStore store = Open(mode);
        DataVoVectorStoreCollection<int, Doc> docs = await SeedDocs(store);

        await Assert.ThrowsAsync<NotSupportedException>(async () => await docs.GetAsync(doc => doc.Category.Length > 3, top: 5).ToListAsync());
        await Assert.ThrowsAsync<NotSupportedException>(async () => await docs.GetAsync(doc => doc.FilePath.EndsWith("_1.md"), top: 5).ToListAsync());
        await Assert.ThrowsAsync<NotSupportedException>(async () => await docs.GetAsync(doc => doc.Tags.Length > 0, top: 5).ToListAsync());
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task FlatEuclidean_FloatArray_LongKeys(StorageMode mode)
    {
        using DataVoVectorStore store = Open(mode);
        var points = new DataVoVectorStoreCollection<long, FlatDoc>(store, "points");
        await points.EnsureCollectionExistsAsync();
        await points.UpsertAsync([
            new FlatDoc { Id = 10_000_000_000, Name = "origin", Embedding = [0, 0, 0] },
            new FlatDoc { Id = 2, Name = "near", Embedding = [1, 0, 0] },
            new FlatDoc { Id = 3, Name = "far", Embedding = [3, 4, 0] },
            new FlatDoc { Id = 4, Name = "none", Embedding = null },
        ]);

        List<VectorSearchResult<FlatDoc>> hits = await points.SearchAsync(new float[] { 0, 0, 0 }, top: 3).ToListAsync();
        Assert.Equal([10_000_000_000L, 2L, 3L], hits.Select(hit => hit.Record.Id));
        Assert.Equal([0d, 1d, 5d], hits.Select(hit => Math.Round(hit.Score!.Value, 5)));

        FlatDoc? none = await points.GetAsync(4, new RecordRetrievalOptions { IncludeVectors = true });
        Assert.NotNull(none);
        Assert.Null(none.Embedding);
        Assert.Equal(new float[] { 3f, 4f, 0f }, (await points.GetAsync(3, new RecordRetrievalOptions { IncludeVectors = true }))!.Embedding);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task GuidKeys_AreGenerated_AndEmbeddingVectorsRoundTrip(StorageMode mode)
    {
        using DataVoVectorStore store = Open(mode);
        var notes = new DataVoVectorStoreCollection<Guid, GuidDoc>(store, "notes");
        await notes.EnsureCollectionExistsAsync();
        var generated = new GuidDoc { Title = "generated", Embedding = new Embedding<float>(new float[] { 1, 0, 0 }) };
        Guid explicitKey = Guid.NewGuid();
        await notes.UpsertAsync([generated, new GuidDoc { Id = explicitKey, Title = "explicit", Embedding = new Embedding<float>(new float[] { 0, 1, 0 }) }]);

        Assert.NotEqual(Guid.Empty, generated.Id);
        GuidDoc? read = await notes.GetAsync(generated.Id, new RecordRetrievalOptions { IncludeVectors = true });
        Assert.NotNull(read);
        Assert.Equal([1f, 0f, 0f], read.Embedding!.Vector.ToArray());

        VectorSearchResult<GuidDoc> nearest = await notes.SearchAsync(new Embedding<float>(new float[] { 0.1f, 1, 0 }), top: 1).SingleAsync();
        Assert.Equal(explicitKey, nearest.Record.Id);
        Assert.True(nearest.Score < 0.05, "cosine distance of near-parallel vectors is small");
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task StringKeys_NullableMemory_EuclideanSquared(StorageMode mode)
    {
        using DataVoVectorStore store = Open(mode);
        var spots = new DataVoVectorStoreCollection<string, StringKeyDoc>(store, "spots");
        await spots.EnsureCollectionExistsAsync();
        await spots.UpsertAsync([
            new StringKeyDoc { Key = "a", Score = 1.5, Point = new ReadOnlyMemory<float>([0, 0]) },
            new StringKeyDoc { Key = "b", Score = 2.5, Point = new ReadOnlyMemory<float>([3, 4]) },
            new StringKeyDoc { Key = "c", Score = 3.5, Point = null },
        ]);

        List<VectorSearchResult<StringKeyDoc>> hits = await spots.SearchAsync(new ReadOnlyMemory<float>([0, 0]), top: 2).ToListAsync();
        Assert.Equal(["a", "b"], hits.Select(hit => hit.Record.Key));
        Assert.Equal(25d, Math.Round(hits[1].Score!.Value, 4)); // squared distance

        StringKeyDoc c = (await spots.GetAsync("c", new RecordRetrievalOptions { IncludeVectors = true }))!;
        Assert.Null(c.Point);
        Assert.Equal(3.5, c.Score);
        Assert.Equal(new float[] { 0f, 0f }, (await spots.GetAsync("a", new RecordRetrievalOptions { IncludeVectors = true }))!.Point!.Value.ToArray());
        Assert.Equal(["b"], (await spots.GetAsync(spot => spot.Score > 2 && spot.Score < 3, top: 10).ToListAsync()).Select(spot => spot.Key));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task EmbeddingGenerator_EmbedsTextOnUpsertAndSearch(StorageMode mode)
    {
        var generator = new TestEmbeddingGenerator();
        using DataVoVectorStore store = Open(mode, new DataVoVectorStoreOptions { EmbeddingGenerator = generator });
        var texts = new DataVoVectorStoreCollection<int, TextDoc>(store, "texts");
        await texts.EnsureCollectionExistsAsync();
        await texts.UpsertAsync([
            new TextDoc { Id = 1, Body = "one", Text = "aaaa bbbb" },
            new TextDoc { Id = 2, Body = "two", Text = "gggg hhhh" },
            new TextDoc { Id = 3, Body = "three", Text = "cccc dddd" },
        ]);
        Assert.Equal(1, generator.Calls); // one batch for the whole upsert

        VectorSearchResult<TextDoc> best = await texts.SearchAsync("ab", top: 1).SingleAsync();
        Assert.Equal(1, best.Record.Id);
        Assert.Equal("one", best.Record.Body);
        Assert.Equal("", best.Record.Text); // the generator's input is not read back

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await texts.SearchAsync("ab", top: 1, new VectorSearchOptions<TextDoc> { IncludeVectors = true }).ToListAsync());
    }

    [Fact]
    public void SchemaErrors_AreReportedWhenTheCollectionIsCreated()
    {
        using DataVoVectorStore store = Open(StorageMode.InMemory);
        Assert.Throws<ArgumentException>(() => new DataVoVectorStoreCollection<int, Doc>(store, "bad-name"));
        Assert.Throws<ArgumentException>(() => new DataVoVectorStoreCollection<int, Doc>(store, "select"));
        Assert.Throws<ArgumentException>(() => new DataVoVectorStoreCollection<long, Doc>(store, "docs"));
        Assert.Throws<NotSupportedException>(() => new DataVoVectorStoreCollection<int, TextDoc>(store, "texts")); // no generator
        var unsupported = new VectorStoreCollectionDefinition
        {
            Properties =
            [
                new VectorStoreKeyProperty("Id", typeof(int)),
                new VectorStoreVectorProperty("Vector", typeof(ReadOnlyMemory<float>), 4) { DistanceFunction = DistanceFunction.DotProductSimilarity },
            ],
        };
        Assert.Throws<NotSupportedException>(() => store.GetDynamicCollection("dots", unsupported));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task DynamicCollection_CrudSearchAndFilter(StorageMode mode)
    {
        using DataVoVectorStore store = Open(mode);
        var definition = new VectorStoreCollectionDefinition
        {
            Properties =
            [
                new VectorStoreKeyProperty("Id", typeof(int)),
                new VectorStoreDataProperty("Category", typeof(string)) { IsIndexed = true },
                new VectorStoreDataProperty("Rank", typeof(int)),
                new VectorStoreVectorProperty("Embedding", typeof(ReadOnlyMemory<float>), 2) { IndexKind = IndexKind.Flat },
            ],
        };
        VectorStoreCollection<object, Dictionary<string, object?>> items = store.GetDynamicCollection("items", definition);
        await items.EnsureCollectionExistsAsync();
        await items.UpsertAsync([
            new Dictionary<string, object?> { ["Id"] = 1, ["Category"] = "x", ["Rank"] = 1, ["Embedding"] = new ReadOnlyMemory<float>([1, 0]) },
            new Dictionary<string, object?> { ["Id"] = 2, ["Category"] = "y", ["Rank"] = 2, ["Embedding"] = new ReadOnlyMemory<float>([0, 1]) },
            new Dictionary<string, object?> { ["Id"] = 3, ["Category"] = "x", ["Rank"] = 3, ["Embedding"] = new ReadOnlyMemory<float>([0.7f, 0.7f]) },
        ]);

        Dictionary<string, object?> read = (await items.GetAsync(3, new RecordRetrievalOptions { IncludeVectors = true }))!;
        Assert.Equal("x", read["Category"]);
        Assert.Equal(3, read["Rank"]);
        Assert.Equal([0.7f, 0.7f], ((ReadOnlyMemory<float>)read["Embedding"]!).ToArray());

        List<VectorSearchResult<Dictionary<string, object?>>> hits = await items.SearchAsync(
            new ReadOnlyMemory<float>([0, 1]),
            top: 5,
            new VectorSearchOptions<Dictionary<string, object?>> { Filter = record => (string)record["Category"]! == "x" }).ToListAsync();
        Assert.Equal([3, 1], hits.Select(hit => (int)hit.Record["Id"]!));

        await items.DeleteAsync(3);
        Assert.Null(await items.GetAsync(3));
        Assert.Equal(["items"], await store.ListCollectionNamesAsync().ToListAsync());
    }

    [Fact]
    public async Task Lsm_CollectionsSurviveReopen()
    {
        string directory = NewDirectory();
        using (var store = new DataVoVectorStore(new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = directory }))
        {
            await SeedDocs(store);
            var flat = new DataVoVectorStoreCollection<long, FlatDoc>(store, "points");
            await flat.EnsureCollectionExistsAsync();
            await flat.UpsertAsync(new FlatDoc { Id = 1, Name = "p", Embedding = [1, 2, 3] });
        }

        using var reopened = new DataVoVectorStore(new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = directory });
        Assert.Equal(["docs", "points"], (await reopened.ListCollectionNamesAsync().ToListAsync()).Order());
        var docs = new DataVoVectorStoreCollection<int, Doc>(reopened, "docs");
        Assert.True(await docs.CollectionExistsAsync());
        Assert.Equal("reference", (await docs.GetAsync(4))!.Category);
        VectorSearchResult<Doc> nearest = await docs.SearchAsync(new float[] { 1, 0.1f, 0, 0 }, top: 1).SingleAsync();
        Assert.Equal(1, nearest.Record.Id);
        List<Doc> filtered = await docs.GetAsync(doc => doc.Category == "guide" && doc.Published, top: 100).ToListAsync();
        Assert.NotEmpty(filtered);
        Assert.All(filtered, doc => Assert.True(doc.Category == "guide" && doc.Published));

        var points = new DataVoVectorStoreCollection<long, FlatDoc>(reopened, "points");
        Assert.Equal(new float[] { 1f, 2f, 3f }, (await points.GetAsync(1, new RecordRetrievalOptions { IncludeVectors = true }))!.Embedding);

        // Writes after reopen go through the same indexes.
        await docs.UpsertAsync(NewDoc(100, "guide", [0, 0, 0, 1]));
        Assert.Equal(100, (await docs.SearchAsync(new float[] { 0, 0, 0, 1 }, top: 1).SingleAsync()).Record.Id);
    }

    [Fact]
    public async Task StoreLevelOperations_AndMetadata()
    {
        using var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        using var store = new DataVoVectorStore(context, new DataVoVectorStoreOptions { DatabaseName = "Vectors" });
        DataVoVectorStoreCollection<int, Doc> docs = await SeedDocs(store);

        Assert.True(await store.CollectionExistsAsync("docs"));
        Assert.Equal("Vectors", Assert.IsType<VectorStoreMetadata>(store.GetService(typeof(VectorStoreMetadata))).VectorStoreName);
        Assert.Same(context, store.GetService(typeof(DataVoContext)));
        VectorStoreCollectionMetadata metadata = Assert.IsType<VectorStoreCollectionMetadata>(docs.GetService(typeof(VectorStoreCollectionMetadata)));
        Assert.Equal("datavo", metadata.VectorStoreSystemName);
        Assert.Equal("docs", metadata.CollectionName);

        // The collection is a plain DataVo table: its rows, its indexes and its distance operator are visible in SQL.
        var rows = context.Execute("USE Vectors; SELECT COUNT(*) AS N FROM docs")[^1];
        Assert.Equal(12L, rows.Data[0]["N"]);
        var plan = context.Execute("USE Vectors; EXPLAIN SELECT Id FROM docs WHERE Category = 'guide'")[^1];
        Assert.False(plan.IsError, string.Join(" ", plan.Messages));

        await store.EnsureCollectionDeletedAsync("docs");
        Assert.False(await store.CollectionExistsAsync("docs"));
    }

    [Fact]
    public async Task DependencyInjection_RegistersStoreCollectionsAndGenerator()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IEmbeddingGenerator<string, Embedding<float>>>(new TestEmbeddingGenerator());
        services.AddDataVoVectorStore(new DataVoConfig { StorageMode = StorageMode.InMemory });
        services.AddDataVoCollection<int, Doc>("docs");
        services.AddDataVoCollection<int, TextDoc>("texts");
        await using ServiceProvider provider = services.BuildServiceProvider();

        VectorStore store = provider.GetRequiredService<VectorStore>();
        Assert.Same(provider.GetRequiredService<DataVoVectorStore>(), store);
        VectorStoreCollection<int, Doc> docs = provider.GetRequiredService<VectorStoreCollection<int, Doc>>();
        Assert.Same(docs, provider.GetRequiredService<IVectorSearchable<Doc>>());
        await docs.EnsureCollectionExistsAsync();
        await docs.UpsertAsync(NewDoc(1, "guide", [1, 0, 0, 0]));
        Assert.Equal(1, (await docs.SearchAsync(new float[] { 1, 0, 0, 0 }, top: 1).SingleAsync()).Record.Id);

        // The registered generator embeds text vector properties.
        VectorStoreCollection<int, TextDoc> texts = provider.GetRequiredService<VectorStoreCollection<int, TextDoc>>();
        await texts.EnsureCollectionExistsAsync();
        await texts.UpsertAsync(new TextDoc { Id = 7, Text = "abc" });
        Assert.Equal(7, (await texts.SearchAsync("abc", top: 1).SingleAsync()).Record.Id);

        var keyed = new ServiceCollection();
        keyed.AddKeyedDataVoVectorStore("cold", new DataVoConfig { StorageMode = StorageMode.InMemory });
        await using ServiceProvider keyedProvider = keyed.BuildServiceProvider();
        Assert.IsType<DataVoVectorStore>(keyedProvider.GetRequiredKeyedService<VectorStore>("cold"));
    }

    [Fact]
    public async Task GetCollection_ThroughTheAbstraction()
    {
        using DataVoVectorStore store = Open(StorageMode.InMemory);
#pragma warning disable IL2026, IL3050 // the abstraction's reflection-based entry point, exercised on purpose
        VectorStoreCollection<int, Doc> docs = store.GetCollection<int, Doc>("docs");
#pragma warning restore IL2026, IL3050
        await docs.EnsureCollectionExistsAsync();
        await docs.UpsertAsync(NewDoc(5, "guide", [0, 1, 0, 0]));
        Assert.Equal(5, (await docs.GetAsync(5))!.Id);
    }

    private DataVoVectorStore Open(StorageMode mode, DataVoVectorStoreOptions? options = null) =>
        new(new DataVoConfig
        {
            StorageMode = mode,
            DiskStoragePath = mode == StorageMode.Lsm ? NewDirectory() : null,
            LsmStrictFsync = false,
        }, options);

    private string NewDirectory()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"datavo_vectordata_{Guid.NewGuid():N}");
        _directories.Add(directory);
        return directory;
    }

    /// <summary>Twelve documents: categories alternate guide/reference, ranks 1-12, one day apart.</summary>
    private static async Task<DataVoVectorStoreCollection<int, Doc>> SeedDocs(DataVoVectorStore store)
    {
        var docs = new DataVoVectorStoreCollection<int, Doc>(store, "docs");
        await docs.EnsureCollectionExistsAsync();
        var random = new Random(7);
        var records = new List<Doc>();
        for (int id = 1; id <= 12; id++)
        {
            float[] vector = id == 1 ? [1, 0.1f, 0, 0] : [(float)random.NextDouble(), (float)random.NextDouble(), (float)random.NextDouble(), (float)random.NextDouble()];
            Doc doc = NewDoc(id, id % 3 == 1 ? "reference" : "guide", vector);
            doc.Note = id % 4 == 0 ? null : $"note {id}";
            doc.Published = id % 2 == 0;
            if (id == 7)
            {
                doc.FilePath = "docs/guide_extra/7.md";
            }

            records.Add(doc);
        }

        await docs.UpsertAsync(records);
        return docs;
    }

    private static Doc NewDoc(int id, string category, float[] vector) => new()
    {
        Id = id,
        Category = category,
        FilePath = $"docs/{(category == "guide" ? "guide" : "reference")}/{id}.md",
        UpdatedAt = Jan.AddDays(id),
        Rank = id,
        Published = true,
        Note = "n",
        Vector = vector,
    };

    private static List<int> Ids<T>(IEnumerable<VectorSearchResult<T>> hits) where T : class =>
        [.. hits.Select(hit => hit.Record switch
        {
            Doc doc => doc.Id,
            _ => throw new InvalidOperationException(),
        })];

    private static double Cosine(ReadOnlySpan<float> a, ReadOnlySpan<float> b)
    {
        double dot = 0, na = 0, nb = 0;
        for (int i = 0; i < a.Length; i++)
        {
            dot += a[i] * b[i];
            na += a[i] * a[i];
            nb += b[i] * b[i];
        }

        return dot / (Math.Sqrt(na) * Math.Sqrt(nb));
    }
}
