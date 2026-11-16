using System.Globalization;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Indexing.HNSW;
using DataVo.Core.Models.Catalog;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// <c>CREATE INDEX ... USING HNSW WITH (...)</c> sets the graph parameters of the index: <c>m</c>,
/// <c>ef_construction</c>, <c>ef_search</c> and <c>enable_diversity_heuristic</c>. The catalog keeps them, so an
/// index rebuilt from its table's rows after a restart is built the same way.
/// </summary>
public sealed class SqlHnswIndexOptionsTests
{
    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    private const int Rows = 120;

    [Theory]
    [MemberData(nameof(Modes))]
    public void Options_ConfigureTheGraphAndAreRecordedInTheCatalog(StorageMode mode)
    {
        using DataVoContext context = Open(mode, out _);
        Seed(context);
        Execute(context, "CREATE INDEX tuned ON Items (Emb) USING HNSW WITH (metric = 'cosine', m = 24, ef_construction = 128, ef_search = 96, enable_diversity_heuristic = false)");
        Execute(context, "CREATE INDEX plain ON Items (Other) USING HNSW");

        HnswIndexOptions options = Assert.IsType<HnswIndexOptions>(CatalogIndex(context, "tuned").Hnsw);
        Assert.Equal((24, 128, 96, false), (options.M, options.EfConstruction, options.EfSearch, options.EnableDiversityHeuristic));
        Assert.Null(CatalogIndex(context, "plain").Hnsw);

        HNSWIndex tuned = Graph(context, "tuned");
        Assert.Equal((24, 128, 96, false), (tuned.M, tuned.EfConstruction, tuned.EfSearch, tuned.EnableDiversityHeuristic));
        Assert.Equal(Rows, tuned.Count);

        // Without options the index keeps its defaults: the diversity heuristic is on.
        HNSWIndex plain = Graph(context, "plain");
        Assert.Equal((16, 64, 64, true), (plain.M, plain.EfConstruction, plain.EfSearch, plain.EnableDiversityHeuristic));

        // The tuned index answers searches.
        QueryResult nearest = context.Execute($"SELECT Id, Emb <=> '{Literal(Vector(17))}' AS dist FROM Items ORDER BY dist LIMIT 1")[^1];
        Assert.False(nearest.IsError, string.Join("; ", nearest.Messages));
        Assert.Equal(17, Convert.ToInt32(Assert.Single(nearest.Data).Values.First(), CultureInfo.InvariantCulture));
    }

    [Fact]
    public void OptionNames_IgnoreCaseAndUnderscores_AndOneOptionLeavesTheOthersDefault()
    {
        using DataVoContext context = Open(StorageMode.InMemory, out _);
        Seed(context);
        Execute(context, "CREATE INDEX a ON Items (Emb) USING HNSW WITH (EnableDiversityHeuristic = FALSE)");
        Execute(context, "CREATE INDEX b ON Items (Other) USING HNSW WITH (EF_CONSTRUCTION = '200', efSearch = 80)");

        HNSWIndex first = Graph(context, "a");
        Assert.Equal((16, 64, 64, false), (first.M, first.EfConstruction, first.EfSearch, first.EnableDiversityHeuristic));
        HNSWIndex second = Graph(context, "b");
        Assert.Equal((16, 200, 80, true), (second.M, second.EfConstruction, second.EfSearch, second.EnableDiversityHeuristic));
    }

    [Fact]
    public void Options_SurviveAReopen_AlsoWhenTheGraphIsRebuiltFromTheRows()
    {
        string directory;
        using (DataVoContext context = Open(StorageMode.Lsm, out directory))
        {
            Seed(context);
            Execute(context, "CREATE INDEX tuned ON Items (Emb) USING HNSW WITH (m = 12, ef_construction = 90, enable_diversity_heuristic = false)");
        }

        using (DataVoContext reopened = Reopen(directory))
        {
            Assert.Equal((12, 90, null, false), OptionsOf(reopened));
            HNSWIndex loaded = Graph(reopened, "tuned");
            Assert.Equal((12, 90, false), (loaded.M, loaded.EfConstruction, loaded.EnableDiversityHeuristic));
        }

        // Without its saved graph the index is rebuilt from the rows, with the options the catalog kept.
        string snapshots = Path.Combine(directory, "HnswOptions", "Items", HnswSnapshotFile.DirectoryName);
        Assert.True(Directory.Exists(snapshots));
        Directory.Delete(snapshots, recursive: true);

        using DataVoContext rebuilt = Reopen(directory);
        Assert.Equal((12, 90, null, false), OptionsOf(rebuilt));
        HNSWIndex graph = Graph(rebuilt, "tuned");
        Assert.Equal((12, 90, false), (graph.M, graph.EfConstruction, graph.EnableDiversityHeuristic));
        Assert.Equal(Rows, graph.Count);
    }

    [Fact]
    public void InvalidOptions_AreRejectedAndCreateNoIndex()
    {
        using DataVoContext context = Open(StorageMode.InMemory, out _);
        Seed(context);
        foreach (string options in new[]
        {
            "m = 1", "m = 129", "m = many", "m = 2.5",
            "ef_construction = 0", "ef_construction = 5000",
            "ef_search = 0", "ef_search = -4",
            "enable_diversity_heuristic = maybe", "enable_diversity_heuristic = 1",
            "m = 8, m = 16",
            "size = 3",
        })
        {
            QueryResult result = context.Execute($"CREATE INDEX bad ON Items (Emb) USING HNSW WITH ({options})")[^1];
            Assert.True(result.IsError, options);
        }

        // The graph options belong to HNSW only.
        Assert.True(context.Execute("CREATE INDEX bad ON Items (Emb) USING FLAT WITH (m = 8)")[^1].IsError);
        Assert.True(context.Execute("CREATE INDEX bad ON Items (Id) WITH (ef_search = 8)")[^1].IsError);
        Assert.DoesNotContain(context.Engine.Catalog.GetTableIndexes("Items", "HnswOptions"), index => index.IndexFileName == "bad");

        // The boundary values are accepted.
        Execute(context, "CREATE INDEX low ON Items (Emb) USING HNSW WITH (m = 2, ef_construction = 1, ef_search = 1)");
        Execute(context, "CREATE INDEX high ON Items (Other) USING HNSW WITH (m = 128, ef_construction = 4096, ef_search = 4096)");
    }

    private static (int? M, int? EfConstruction, int? EfSearch, bool? Diversity) OptionsOf(DataVoContext context)
    {
        HnswIndexOptions options = Assert.IsType<HnswIndexOptions>(CatalogIndex(context, "tuned").Hnsw);
        return (options.M, options.EfConstruction, options.EfSearch, options.EnableDiversityHeuristic);
    }

    private static IndexFile CatalogIndex(DataVoContext context, string name) =>
        context.Engine.Catalog.GetTableIndexes("Items", "HnswOptions").Single(index => index.IndexFileName == name);

    private static HNSWIndex Graph(DataVoContext context, string name) =>
        context.Engine.IndexManager.TryGetHnswVectorIndex(name, "Items", "HnswOptions")
        ?? throw new InvalidOperationException($"Index {name} is not an HNSW graph.");

    private static DataVoContext Open(StorageMode mode, out string directory)
    {
        directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("sql_hnsw_options") : string.Empty;
        var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = mode == StorageMode.Lsm ? directory : null });
        Execute(context, "CREATE DATABASE HnswOptions");
        Execute(context, "USE HnswOptions");
        return context;
    }

    private static DataVoContext Reopen(string directory)
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = directory });
        Execute(context, "USE HnswOptions");
        return context;
    }

    private static void Seed(DataVoContext context)
    {
        Execute(context, "CREATE TABLE Items (Id INT PRIMARY KEY, Emb VECTOR(8), Other VECTOR(8))");
        Execute(context, "INSERT INTO Items (Id, Emb, Other) VALUES " + string.Join(", ", Enumerable.Range(1, Rows).Select(id =>
            $"({id}, '{Literal(Vector(id))}', '{Literal(Vector(id + 1_000))}')")));
    }

    private static float[] Vector(int seed)
    {
        var random = new Random(seed);
        return [.. Enumerable.Range(0, 8).Select(_ => (float)(random.NextDouble() * 2 - 1))];
    }

    private static string Literal(float[] vector) =>
        "[" + string.Join(", ", vector.Select(x => x.ToString("R", CultureInfo.InvariantCulture))) + "]";

    private static void Execute(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql[..Math.Min(sql.Length, 140)] + ": " + string.Join("; ", result.Messages));
    }
}
