using DataVo.Core.Indexing.HNSW;

namespace DataVo.Tests.Indexing;

public class HNSWIndexTests : IDisposable
{
    private readonly string _tempDir;

    public HNSWIndexTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"datavo_hnsw_tests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    [Fact]
    public void Insert_BuildsLayeredGraphState()
    {
        var index = new HNSWIndex
        {
            Metric = "cosine",
            M = 8,
            EfConstruction = 32,
            EfSearch = 24
        };

        index.Insert(1, [1f, 0f, 0f]);
        index.Insert(2, [0.95f, 0.05f, 0f]);
        index.Insert(3, [0f, 1f, 0f]);
        index.Insert(4, [0f, 0f, 1f]);

        Assert.Equal(4, index.Count);
        Assert.NotNull(index.EntryPointId);
        Assert.True(index.MaxLevel >= 0);
        Assert.NotEmpty(index.NodeLevels);
        Assert.NotEmpty(index.Layers);
        Assert.All(index.Layers.Values.SelectMany(level => level.Values), neighbors =>
        {
            Assert.True(neighbors.Count <= index.M);
        });

        Assert.True(index.Layers.TryGetValue(0, out var levelZero));
        Assert.NotNull(levelZero);
        Assert.All(levelZero!, pair =>
        {
            if (index.Count > 1)
            {
                Assert.NotEmpty(pair.Value);
            }
        });
    }

    [Fact]
    public void SearchTopK_ReturnsNearestCandidates()
    {
        var index = new HNSWIndex
        {
            Metric = "cosine",
            EfSearch = 32
        };

        index.Insert(10, [1f, 0f, 0f]);
        index.Insert(20, [0f, 1f, 0f]);
        index.Insert(30, [0f, 0f, 1f]);

        List<long> nearest = index.SearchTopK([0.98f, 0.02f, 0f], 2);

        Assert.Equal(2, nearest.Count);
        Assert.Equal(10, nearest[0]);

        List<long> mismatch = index.SearchTopK([1f, 0f], 2);
        Assert.Empty(mismatch);
    }

    [Fact]
    public void Persistence_RoundTrip_PreservesGraphAndParameters()
    {
        var index = new HNSWIndex
        {
            Metric = "euclidean",
            M = 10,
            EfConstruction = 40,
            EfSearch = 30,
            EnableDiversityHeuristic = false
        };

        index.Insert(1, [1f, 0f, 0f]);
        index.Insert(2, [0.9f, 0.1f, 0f]);
        index.Insert(3, [0f, 1f, 0f]);
        index.Insert(4, [0f, 0f, 1f]);

        var persistence = new HNSWIndexPersistence();
        string filePath = Path.Combine(_tempDir, "idx.vector.json");

        persistence.SaveIndex(index, filePath);
        var loaded = (HNSWIndex)persistence.LoadIndex(filePath);

        Assert.Equal("euclidean", loaded.Metric);
        Assert.Equal(10, loaded.M);
        Assert.Equal(40, loaded.EfConstruction);
        Assert.Equal(30, loaded.EfSearch);
        Assert.False(loaded.EnableDiversityHeuristic);
        Assert.Equal(index.Count, loaded.Count);
        Assert.NotNull(loaded.EntryPointId);
        Assert.NotEmpty(loaded.NodeLevels);
        Assert.NotEmpty(loaded.Layers);

        List<long> nearest = loaded.SearchTopK([0.95f, 0.05f, 0f], 1);
        Assert.Single(nearest);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, recursive: true);
        }
    }
}
