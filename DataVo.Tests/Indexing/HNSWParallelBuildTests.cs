using DataVo.Core;
using DataVo.Core.Indexing.HNSW;

namespace DataVo.Tests.Indexing;

/// <summary>
/// Guards the parallel HNSW build against nodes that no search can reach.
/// </summary>
/// <remarks>
/// A parallel build used to leave a few nodes per 2,000 with no edge pointing at them, so a search
/// for a node's exact vector could not find it. Concurrent inserts published their reverse edges
/// top level first, which let another insert descend into a node whose level-0 list was still
/// empty; with the diversity heuristic, two close nodes inserted together could also have every
/// reverse edge rejected. Each build here is checked two ways: every node is reachable from the
/// entry point on every level it lives on, and a search for every inserted vector returns a row at
/// distance zero. Thread scheduling varies from run to run, but the build guarantees both.
/// </remarks>
public class HNSWParallelBuildTests
{
    private const float Epsilon = 1e-5f;

    [Theory]
    [InlineData("euclidean", 3, false)]  // the spatial VEC3 case that first showed the bug
    [InlineData("euclidean", 3, true)]
    [InlineData("cosine", 16, false)]
    [InlineData("cosine", 16, true)]
    public void InsertBatchParallel_EveryInsertedVectorIsFoundByAnExactSearch(string metric, int dimension, bool diversity)
    {
        const int count = 2_500;
        for (int round = 0; round < 3; round++)
        {
            (RowKey[] ids, float[] vectors) = RandomVectors(count, dimension, seed: 7_000 + round);
            HNSWIndex index = CreateIndex(metric, diversity);

            index.InsertBatchParallel(ids, vectors, dimension, maxDegreeOfParallelism: Math.Max(4, Environment.ProcessorCount));

            Assert.Equal(count, index.Count);
            Assert.Equal(0, index.CountUnreachableNodes());
            AssertEveryVectorFindsItself(index, ids, vectors, dimension, metric);
        }
    }

    [Fact]
    public void InsertBatchParallel_IntoAnExistingIndex_KeepsOldAndNewRowsReachable()
    {
        const int dimension = 3;
        (RowKey[] ids, float[] vectors) = RandomVectors(3_000, dimension, seed: 42);
        HNSWIndex index = CreateIndex("euclidean", diversity: false);

        // Rows already in the index, then a second batch on top - the CREATE INDEX + INSERT shape.
        for (int i = 0; i < 1_000; i++)
        {
            index.Insert(ids[i], vectors.AsSpan(i * dimension, dimension));
        }

        index.InsertBatchParallel(ids[1_000..], vectors[(1_000 * dimension)..], dimension);

        Assert.Equal(3_000, index.Count);
        Assert.Equal(0, index.CountUnreachableNodes());
        AssertEveryVectorFindsItself(index, ids, vectors, dimension, "euclidean");
    }

    [Fact]
    public void RepairConnectivity_RelinksANodeThatLostEveryInboundEdge()
    {
        const int dimension = 3;
        const int count = 600;
        (RowKey[] ids, float[] vectors) = RandomVectors(count, dimension, seed: 11);
        HNSWIndex built = CreateIndex("euclidean", diversity: false);
        for (int i = 0; i < count; i++)
        {
            built.Insert(ids[i], vectors.AsSpan(i * dimension, dimension));
        }

        // Cut every edge that points at one node, on every level: the exact damage the race did.
        HNSWIndex.FlatState state = built.ExportFlatState();
        int victimPosition = count / 2;
        if (ids[victimPosition] == state.EntryPointId)
        {
            victimPosition++; // the entry point is reachable by definition
        }

        int victim = Array.IndexOf(state.RowIdByOrdinal, ids[victimPosition]);
        int stride = state.LevelStrides.Sum();
        int nodes = state.GraphLinks.Length / stride;
        for (int ordinal = 0; ordinal < nodes; ordinal++)
        {
            for (int level = 0; level < state.LevelStrides.Length; level++)
            {
                int start = ordinal * stride + state.LevelOffsets[level];
                Span<int> list = state.GraphLinks.AsSpan(start, state.LevelStrides[level]);
                int kept = 0;
                for (int i = 0; i < list.Length && list[i] >= 0; i++)
                {
                    if (list[i] != victim)
                    {
                        list[kept++] = list[i];
                    }
                }

                list[kept..].Fill(-1);
            }
        }

        var damaged = new HNSWIndex();
        damaged.ImportFlatState(state);
        ReadOnlySpan<float> victimVector = vectors.AsSpan(victimPosition * dimension, dimension);
        Assert.True(damaged.CountUnreachableNodes() > 0);
        Assert.NotEqual(ids[victimPosition], damaged.SearchTopK(victimVector, 1).Single());

        Assert.True(damaged.RepairConnectivity() > 0);

        Assert.Equal(0, damaged.CountUnreachableNodes());
        AssertEveryVectorFindsItself(damaged, ids, vectors, dimension, "euclidean");
        Assert.Equal(0, damaged.RepairConnectivity()); // nothing left to fix
    }

    [Fact]
    public void InsertBatchParallel_AfterReloadingAPersistedIndex_KeepsNewRowsReachable()
    {
        // Loading used to leave the per-node locks unset; every reverse edge write then bailed out,
        // so rows inserted after a reload had no inbound edges and no search could find them.
        const int dimension = 3;
        (RowKey[] ids, float[] vectors) = RandomVectors(2_000, dimension, seed: 99);
        HNSWIndex original = CreateIndex("euclidean", diversity: false);
        original.InsertBatchParallel(ids[..1_000], vectors[..(1_000 * dimension)], dimension);

        string directory = Path.Combine(Path.GetTempPath(), $"datavo_hnsw_parallel_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        try
        {
            var persistence = new HNSWIndexPersistence();
            string path = Path.Combine(directory, "idx.vector.json");
            persistence.SaveIndex(original, path);
            var loaded = (HNSWIndex)persistence.LoadIndex(path);

            loaded.InsertBatchParallel(ids[1_000..], vectors[(1_000 * dimension)..], dimension);

            Assert.Equal(2_000, loaded.Count);
            Assert.Equal(0, loaded.CountUnreachableNodes());
            AssertEveryVectorFindsItself(loaded, ids, vectors, dimension, "euclidean");
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static HNSWIndex CreateIndex(string metric, bool diversity)
    {
        var index = (HNSWIndex)new HNSWIndexFactory().CreateIndex("idx", "embedding", new Dictionary<string, object> { ["metric"] = metric });
        index.EnableDiversityHeuristic = diversity;
        return index;
    }

    private static (RowKey[] Ids, float[] Vectors) RandomVectors(int count, int dimension, int seed)
    {
        var random = new Random(seed);
        var ids = new RowKey[count];
        var vectors = new float[count * dimension];
        for (int i = 0; i < count; i++)
        {
            ids[i] = i + 1;
            for (int d = 0; d < dimension; d++)
            {
                vectors[i * dimension + d] = (float)(random.NextDouble() * 240 - 120);
            }
        }

        return (ids, vectors);
    }

    private static void AssertEveryVectorFindsItself(HNSWIndex index, RowKey[] ids, float[] vectors, int dimension, string metric)
    {
        var positionById = new Dictionary<RowKey, int>(ids.Length);
        for (int i = 0; i < ids.Length; i++)
        {
            positionById[ids[i]] = i;
        }

        var unreachable = new List<RowKey>();
        for (int i = 0; i < ids.Length; i++)
        {
            ReadOnlySpan<float> query = vectors.AsSpan(i * dimension, dimension);
            List<RowKey> hits = index.SearchTopK(query, 1);
            bool found = hits.Count == 1
                && Distance(query, vectors.AsSpan(positionById[hits[0]] * dimension, dimension), metric) <= Epsilon;
            if (!found)
            {
                unreachable.Add(ids[i]);
            }
        }

        Assert.True(unreachable.Count == 0, $"{unreachable.Count} of {ids.Length} rows were unreachable: {string.Join(", ", unreachable.Take(10))}");
    }

    private static float Distance(ReadOnlySpan<float> left, ReadOnlySpan<float> right, string metric)
    {
        double dot = 0, leftNorm = 0, rightNorm = 0, squared = 0;
        for (int i = 0; i < left.Length; i++)
        {
            dot += left[i] * right[i];
            leftNorm += left[i] * left[i];
            rightNorm += right[i] * right[i];
            double delta = left[i] - right[i];
            squared += delta * delta;
        }

        return metric == "cosine"
            ? (float)(1 - dot / Math.Sqrt(leftNorm * rightNorm))
            : (float)Math.Sqrt(squared);
    }
}
