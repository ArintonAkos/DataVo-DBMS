using DataVo.Core.Indexing.HNSW;

namespace DataVo.Tests.Indexing;

/// <summary>
/// Characterization + guard tests pinning the behavior of the HNSW backing store
/// across the paged (chunked) storage refactor.
///
/// The address math (page = ordinal &gt;&gt; shift, slot = ordinal &amp; mask) must return
/// byte-identical vectors and neighbor spans as the previous contiguous arrays. These
/// tests deliberately use a high dimension / many vectors so that ordinals span MANY
/// page boundaries, and they avoid <c>Reserve</c> so the streaming single-insert path
/// (the one Scenario C uses) is exercised without pre-sizing.
/// </summary>
public class HNSWPagedBackingTests
{
    // dim=1536 -> a vector page holds 8 ordinals (8*1536*4 = 49,152 B < 85,000 LOH).
    private const int ProductionDimension = 1536;

    [Fact]
    public void Vectors_RoundTripExactly_AcrossManyVectorPageBoundaries()
    {
        // 200 ordinals at dim=1536 => ~25 vector pages: every page boundary is crossed.
        const int vectors = 200;
        var rng = new Random(20260623);

        var index = new HNSWIndex { Metric = "cosine", M = 16, EfConstruction = 100, EfSearch = 64 };
        var data = new Dictionary<long, float[]>(vectors);

        for (int id = 1; id <= vectors; id++)
        {
            float[] vector = RandomUnitVector(rng, ProductionDimension);
            data[id] = vector;
            index.Insert(id, vector);
        }

        // Self-query in EXACT mode (topK >= count). The stored vector for an ordinal must be
        // byte-identical, so its own distance is exactly 0 and it MUST rank first. A paging
        // address bug returns a different page's bytes -> wrong nearest -> failure. Fully
        // deterministic (no dependence on Random.Shared graph structure).
        foreach ((long id, float[] vector) in data)
        {
            List<long> ranked = index.SearchTopK(vector, vectors);
            Assert.Equal(id, ranked[0]);
        }
    }

    [Fact]
    public void Graph_Navigation_SurvivesPaging_SelfQueryReachableAcrossGraphPages()
    {
        // M=16 => node graph stride ~544 ints (~2,176 B) => a graph page holds 32 ordinals.
        // 400 ordinals => ~13 graph pages, so the graph address math is exercised across pages.
        const int vectors = 400;
        const int dimension = 16;
        var rng = new Random(20260624);

        var index = new HNSWIndex
        {
            Metric = "cosine",
            M = 16,
            EfConstruction = 96,
            EfSearch = 96,
            EnableDiversityHeuristic = true
        };

        var data = new Dictionary<long, float[]>(vectors);
        for (int id = 1; id <= vectors; id++)
        {
            float[] vector = RandomUnitVector(rng, dimension);
            data[id] = vector;
            index.Insert(id, vector);
        }

        // ANN mode (topK < count) traverses the paged neighbor graph. Each node's own vector
        // has distance 0, so a correctly navigable graph finds the node itself. A graph-paging
        // bug corrupts neighbor spans -> navigation breaks -> self becomes unreachable.
        int found = 0;
        foreach ((long id, float[] vector) in data)
        {
            List<long> ann = index.SearchTopK(vector, 10);
            if (ann.Contains(id))
            {
                found++;
            }
        }

        Assert.True(found >= vectors - 2, $"Expected self reachable for >= {vectors - 2} nodes, got {found}.");
    }

    [Fact]
    public void Persistence_RoundTrip_PreservesVectorsExactly_AcrossPages()
    {
        const int vectors = 120; // ~15 vector pages at dim=1536.
        var rng = new Random(20260625);

        var index = new HNSWIndex { Metric = "cosine", M = 12, EfConstruction = 80, EfSearch = 64 };
        var data = new Dictionary<long, float[]>(vectors);
        for (int id = 1; id <= vectors; id++)
        {
            float[] vector = RandomUnitVector(rng, ProductionDimension);
            data[id] = vector;
            index.Insert(id, vector);
        }

        string tempDir = Path.Combine(Path.GetTempPath(), $"datavo_hnsw_paging_{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempDir);
        try
        {
            var persistence = new HNSWIndexPersistence();
            string filePath = Path.Combine(tempDir, "idx.vector.json");
            persistence.SaveIndex(index, filePath);
            var loaded = (HNSWIndex)persistence.LoadIndex(filePath);

            Assert.Equal(index.Count, loaded.Count);

            // Flatten (export) -> page (import) must preserve every stored vector exactly.
            foreach ((long id, float[] vector) in data)
            {
                List<long> ranked = loaded.SearchTopK(vector, vectors);
                Assert.Equal(id, ranked[0]);
            }
        }
        finally
        {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    [Fact]
    public void Insert_StreamingPath_AllocatesBelowBudget()
    {
        // Streaming single-insert path (no Reserve), dim=1536 — the path Scenario C uses.
        // Contiguous array doubling allocated ~18,500 B/insert (store-vector churn + closures).
        // Paged backing + de-closured edge connection must bring this well under budget.
        const int vectors = 400;
        const int budgetBytesPerInsert = 11_000;

        var rng = new Random(20260626);
        var data = new float[vectors][];
        for (int i = 0; i < vectors; i++)
        {
            data[i] = RandomUnitVector(rng, ProductionDimension);
        }

        var index = new HNSWIndex { Metric = "cosine", M = 16, EfConstruction = 100, EfSearch = 64 };

        // Warm up one insert so one-time level-layout / workspace allocations are excluded.
        index.Insert(1, data[0]);

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 1; i < vectors; i++)
        {
            index.Insert(i + 1, data[i]);
        }
        long after = GC.GetAllocatedBytesForCurrentThread();

        double bytesPerInsert = (double)(after - before) / (vectors - 1);
        Assert.True(
            bytesPerInsert < budgetBytesPerInsert,
            $"Expected < {budgetBytesPerInsert} B/insert (dim={ProductionDimension}, streaming), got {bytesPerInsert:F0}.");
    }

    private static float[] RandomUnitVector(Random rng, int dimension)
    {
        float[] vector = new float[dimension];
        double norm = 0d;
        for (int i = 0; i < dimension; i++)
        {
            float value = (float)(rng.NextDouble() - 0.5d);
            vector[i] = value;
            norm += value * value;
        }

        float inv = norm > 0d ? (float)(1d / Math.Sqrt(norm)) : 1f;
        for (int i = 0; i < dimension; i++)
        {
            vector[i] *= inv;
        }

        return vector;
    }
}
