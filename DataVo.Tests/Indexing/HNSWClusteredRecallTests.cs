using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Indexing.HNSW;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Indexing;

/// <summary>
/// Guards HNSW recall on strongly clustered vectors, where a search can end in the wrong cluster.
/// </summary>
/// <remarks>
/// The data are 5,000 unit vectors of 64 dimensions drawn around 64 random centres, with per-component
/// noise of 0.8/sqrt(dimensions), and 200 held-out queries drawn the same way (the shape of the
/// <c>hybrid-vector</c> benchmark, made smaller). When every node linked only to its nearest neighbors,
/// almost no edge left a cluster: a search that entered the wrong cluster stayed there, and 4 to 13 of
/// the 200 queries found none of their true top-10 at any search width. The diversity heuristic, now on
/// by default, keeps edges toward other clusters. The dataset is seeded; level sampling and thread
/// scheduling are not, so each assertion leaves a margin that every build meets.
/// </remarks>
public sealed class HNSWClusteredRecallTests
{
    private const int Documents = 5_000;
    private const int Dimensions = 64;
    private const int Clusters = 64;
    private const int Queries = 200;
    private const int TopK = 10;

    private static readonly Lazy<ClusteredData> Data = new(() => ClusteredData.Create(Documents, Dimensions, Clusters, Queries, seed: 20261115));

    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void DefaultIndex_FindsEveryQuerysOwnCluster(bool parallelBatches)
    {
        ClusteredData data = Data.Value;
        var index = (HNSWIndex)new HNSWIndexFactory().CreateIndex("idx", "emb", new Dictionary<string, object> { ["metric"] = "cosine" });

        if (parallelBatches)
        {
            // The SQL load inserts in batches, each built in parallel and then repaired.
            for (int start = 0; start < Documents; start += 1_000)
            {
                index.InsertBatchParallel(data.Ids[start..(start + 1_000)], data.Flat[(start * Dimensions)..((start + 1_000) * Dimensions)], Dimensions);
            }
        }
        else
        {
            for (int i = 0; i < Documents; i++)
            {
                index.Insert(data.Ids[i], data.Flat.AsSpan(i * Dimensions, Dimensions));
            }
        }

        AssertRecall(data, query => index.SearchTopK(data.Queries[query], TopK).Select(id => (long)id));
    }

    [Theory]
    [InlineData(64)]
    [InlineData(512)]
    public void DefaultIndex_AtAFixedSearchWidth_FindsEveryQuerysOwnCluster(int efSearch)
    {
        ClusteredData data = Data.Value;
        var index = new HNSWIndex { Metric = "cosine" };
        index.InsertBatchParallel(data.Ids, data.Flat, Dimensions);
        index.EnableAdaptiveEfSearch = false;
        index.EfSearch = efSearch;

        AssertRecall(data, query => index.SearchTopK(data.Queries[query], TopK).Select(id => (long)id));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void SqlHnswIndex_WithDefaultParameters_FindsEveryQuerysOwnCluster(StorageMode mode)
    {
        ClusteredData data = Data.Value;
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("hnsw_clustered_recall") : null;
        try
        {
            using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = directory, LsmStrictFsync = false });
            Execute(context, "CREATE DATABASE Clustered");
            Execute(context, "USE Clustered");
            Execute(context, $"CREATE TABLE Docs (Id INT PRIMARY KEY, Emb VECTOR({Dimensions}))");
            Execute(context, "CREATE INDEX vx ON Docs (Emb) USING HNSW");
            var schema = new ReactiveRowSchema("Id", "Emb");
            for (int start = 0; start < Documents; start += 1_000)
            {
                context.InsertTypedBatch("Docs", schema, [.. Enumerable.Range(start, 1_000).Select(i => new[]
                {
                    CellValue.From((int)data.Ids[i]),
                    CellValue.FromVectorOwned(data.Flat.AsSpan(i * Dimensions, Dimensions).ToArray()),
                })]);
            }

            DataVoPreparedStatement statement = context.Prepare($"SELECT Id, Emb <=> @q AS dist FROM Docs ORDER BY dist LIMIT {TopK}");
            context.Diagnostics.Enabled = true;
            AssertRecall(data, query =>
            {
                QueryResult result = statement.Execute(DataVoQueryParameter.FromObject("q", data.Queries[query]));
                Assert.False(result.IsError, string.Join("; ", result.Messages));
                Assert.True(context.Diagnostics.LastQuery?.VectorIndexUsed, "the query did not use the HNSW index");
                return result.Data.Select(row => Convert.ToInt64(row["Id"]));
            });
        }
        finally
        {
            if (directory != null)
            {
                try
                {
                    Directory.Delete(directory, recursive: true);
                }
                catch (IOException)
                {
                }
            }
        }
    }

    /// <summary>
    /// Nearest-neighbor-only graphs are clique-like: the nodes of a cluster point at each other and few
    /// edges come in. The connectivity repair used to free a slot by dropping any edge whose target had
    /// two incoming edges, which could drop the only edge into such a clique; a parallel build of this
    /// data then left 30 to 156 nodes that no search could reach. It now keeps a spanning tree intact.
    /// (Reachable is all this checks: such a graph still strands searches in the wrong cluster, which is
    /// why the diversity heuristic is the default.)
    /// </summary>
    [Fact]
    public void NearestOnlyGraph_ParallelBuild_LeavesNoNodeUnreachable()
    {
        ClusteredData data = Data.Value;
        for (int round = 0; round < 3; round++)
        {
            var index = new HNSWIndex { Metric = "cosine", EnableDiversityHeuristic = false };

            index.InsertBatchParallel(data.Ids, data.Flat, Dimensions, maxDegreeOfParallelism: Math.Max(4, Environment.ProcessorCount));

            Assert.Equal(0, index.CountUnreachableNodes());
        }
    }

    [Fact]
    public void RepairConnectivity_OnAConnectedGraph_AddsNothing()
    {
        ClusteredData data = Data.Value;
        var index = new HNSWIndex { Metric = "cosine", EnableDiversityHeuristic = false };
        index.InsertBatchParallel(data.Ids, data.Flat, Dimensions);

        Assert.Equal(0, index.RepairConnectivity());
        Assert.Equal(0, index.CountUnreachableNodes());
    }

    // No query may lose its own cluster (recall below 50%), and recall over all queries stays near 100%.
    private static void AssertRecall(ClusteredData data, Func<int, IEnumerable<long>> search)
    {
        var poor = new List<string>();
        double total = 0;
        for (int query = 0; query < Queries; query++)
        {
            var found = new HashSet<long>(search(query));
            int hits = data.TrueTopK[query].Count(found.Contains);
            total += hits / (double)TopK;
            if (hits < TopK / 2)
            {
                poor.Add($"q{query}: {hits}/{TopK}");
            }
        }

        Assert.True(poor.Count == 0, $"{poor.Count} of {Queries} queries found fewer than half of their true top-{TopK}: {string.Join(", ", poor.Take(10))}");
        Assert.True(total / Queries >= 0.98, $"mean recall@{TopK} was {total / Queries:P1}");
    }

    private static void Execute(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
    }

    private sealed class ClusteredData
    {
        public required RowKey[] Ids { get; init; }

        public required float[] Flat { get; init; }

        public required float[][] Queries { get; init; }

        public required long[][] TrueTopK { get; init; }

        public static ClusteredData Create(int count, int dimensions, int clusters, int queries, int seed)
        {
            var random = new Random(seed);
            float[][] centers = [.. Enumerable.Range(0, clusters).Select(_ => Normalize([.. Enumerable.Range(0, dimensions).Select(_ => (float)(random.NextDouble() * 2 - 1))]))];
            float noise = 0.8f / MathF.Sqrt(dimensions);
            float[] Draw()
            {
                float[] center = centers[random.Next(clusters)];
                var vector = new float[dimensions];
                for (int d = 0; d < dimensions; d++)
                {
                    vector[d] = center[d] + (float)(random.NextDouble() * 2 - 1) * noise;
                }

                return Normalize(vector);
            }

            var ids = new RowKey[count];
            var flat = new float[count * dimensions];
            for (int i = 0; i < count; i++)
            {
                ids[i] = i + 1;
                Draw().CopyTo(flat, i * dimensions);
            }

            float[][] queryVectors = [.. Enumerable.Range(0, queries).Select(_ => Draw())];
            var data = new ClusteredData { Ids = ids, Flat = flat, Queries = queryVectors, TrueTopK = [] };
            return new ClusteredData
            {
                Ids = ids,
                Flat = flat,
                Queries = queryVectors,
                TrueTopK = [.. queryVectors.Select(query => Enumerable.Range(1, count).OrderBy(id => data.Distance(query, id)).ThenBy(id => id).Take(TopK).Select(id => (long)id).ToArray())],
            };
        }

        // Cosine distance; every vector is unit length.
        public float Distance(ReadOnlySpan<float> query, long id)
        {
            ReadOnlySpan<float> row = Flat.AsSpan((int)(id - 1) * Dimensions, Dimensions);
            float dot = 0;
            for (int d = 0; d < row.Length; d++)
            {
                dot += query[d] * row[d];
            }

            return 1f - dot;
        }

        private static float[] Normalize(float[] vector)
        {
            float norm = MathF.Sqrt(vector.Sum(x => x * x));
            for (int d = 0; d < vector.Length; d++)
            {
                vector[d] /= norm;
            }

            return vector;
        }
    }
}
