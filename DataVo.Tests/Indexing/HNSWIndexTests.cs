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

    [Fact]
    public void Delete_RepairsLocalNeighborhood_WhenEnabled()
    {
        var index = new HNSWIndex
        {
            Metric = "euclidean",
            M = 8,
            EnableDeleteGraphRepair = true,
            EnableDiversityHeuristic = false
        };

        index.Entries = new Dictionary<long, float[]>
        {
            [1] = [0f, 0f],
            [2] = [1f, 0f],
            [3] = [2f, 0f]
        };

        index.NodeLevels = new Dictionary<long, int>
        {
            [1] = 0,
            [2] = 0,
            [3] = 0
        };

        index.Layers = new Dictionary<int, Dictionary<long, List<long>>>
        {
            [0] = new Dictionary<long, List<long>>
            {
                [1] = [2],
                [2] = [1, 3],
                [3] = [2]
            }
        };

        index.EntryPointId = 2;
        index.MaxLevel = 0;

        index.Delete([2]);

        Assert.True(index.Layers.TryGetValue(0, out var level0));
        Assert.NotNull(level0);
        Assert.True(level0!.TryGetValue(1, out var n1));
        Assert.True(level0.TryGetValue(3, out var n3));
        Assert.Contains(3, n1!);
        Assert.Contains(1, n3!);
    }

    [Fact]
    public void Benchmark_RecallAt10_AgainstExactBaseline_IsReasonable()
    {
        var index = new HNSWIndex
        {
            Metric = "cosine",
            M = 12,
            EfConstruction = 96,
            EfSearch = 96,
            EnableDiversityHeuristic = true,
            EnableDeleteGraphRepair = true
        };

        var random = new Random(20260324);
        const int dimension = 16;
        const int vectors = 600;
        const int queries = 40;
        const int topK = 10;

        for (int id = 1; id <= vectors; id++)
        {
            float[] vector = new float[dimension];
            for (int i = 0; i < dimension; i++)
            {
                vector[i] = (float)random.NextDouble();
            }

            index.Insert(id, vector);
        }

        double totalRecall = 0d;

        for (int q = 0; q < queries; q++)
        {
            float[] query = new float[dimension];
            for (int i = 0; i < dimension; i++)
            {
                query[i] = (float)random.NextDouble();
            }

            List<long> ann = index.SearchTopK(query, topK);
            List<long> exact = index.Entries
                .Select(entry => (entry.Key, DistanceCosine(query, entry.Value)))
                .OrderBy(item => item.Item2)
                .ThenBy(item => item.Key)
                .Take(topK)
                .Select(item => item.Key)
                .ToList();

            int overlap = ann.Intersect(exact).Count();
            totalRecall += (double)overlap / topK;
        }

        double avgRecall = totalRecall / queries;
        Assert.True(avgRecall >= 0.60d, $"Expected avg recall@10 >= 0.60, got {avgRecall:F3}");
    }

    private static float DistanceCosine(float[] a, float[] b)
    {
        float dot = 0f;
        float ma = 0f;
        float mb = 0f;

        for (int i = 0; i < a.Length; i++)
        {
            dot += a[i] * b[i];
            ma += a[i] * a[i];
            mb += b[i] * b[i];
        }

        ma = MathF.Sqrt(ma);
        mb = MathF.Sqrt(mb);
        if (ma == 0f || mb == 0f)
        {
            return 1f;
        }

        return 1f - (dot / (ma * mb));
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, recursive: true);
        }
    }
}
