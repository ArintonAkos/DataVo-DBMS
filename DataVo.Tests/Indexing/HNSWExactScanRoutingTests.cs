using DataVo.Core;
using DataVo.Core.Indexing;
using DataVo.Core.Indexing.Flat;
using DataVo.Core.Indexing.HNSW;

namespace DataVo.Tests.Indexing;

/// <summary>
/// An HNSW search for many candidates (what a selective filter asks for) widens its beam until walking the
/// graph costs more than scanning the stored vectors. The index then scans, which is also exact. These tests
/// check when it does so, that the scan returns the exact nearest rows, and that both vector indexes rank a
/// given set of rows by exact distance.
/// </summary>
public sealed class HNSWExactScanRoutingTests
{
    private const int Dimensions = 32;
    private const int Vectors = 4_000;

    [Fact]
    public void FewNeighbors_SearchTheGraph_ManyNeighbors_ScanExactly()
    {
        HNSWIndex index = Build(out _);

        // A beam of 108 over 4,000 vectors of 32 dimensions is cheaper on the graph; a beam of 2,155 is not.
        Assert.False(index.PrefersExactScan(1));
        Assert.True(index.PrefersExactScan(120));
        Assert.True(index.PrefersExactScan(Vectors));
        Assert.False(index.PrefersExactScan(0));

        // The threshold grows with the index: the same request stays on the graph of a larger index.
        index.EnableExactScanCostRouting = false;
        Assert.False(index.PrefersExactScan(120));
        Assert.True(index.PrefersExactScan(Vectors)); // every row is asked for: nothing to search
    }

    [Fact]
    public void RoutedSearch_ReturnsTheExactNearestRowsInOrder()
    {
        HNSWIndex index = Build(out float[][] vectors);
        foreach (int seed in new[] { 11, 12, 13 })
        {
            float[] query = Vector(seed);
            foreach (int topK in new[] { 120, 500, Vectors })
            {
                Assert.True(index.PrefersExactScan(topK));
                double[] exact = [.. Enumerable.Range(1, Vectors).Select(id => Cosine(vectors[id], query)).Order().Take(topK)];
                long[] found = [.. index.SearchTopK(query, topK).Select(key => key.AsInt64())];

                // The index compares single-precision distances, so rows a rounding error apart may swap places:
                // the i-th returned row must be as near as the i-th nearest row, within that error.
                Assert.Equal(topK, found.Distinct().Count());
                for (int i = 0; i < topK; i++)
                {
                    Assert.Equal(exact[i], Cosine(vectors[found[i]], query), 5);
                }
            }
        }
    }

    [Fact]
    public void RoutingOff_StillAnswersFromTheGraph()
    {
        HNSWIndex index = Build(out float[][] vectors);
        index.EnableExactScanCostRouting = false;
        float[] query = Vector(21);
        HashSet<long> exact = [.. Enumerable.Range(1, Vectors).OrderBy(id => Cosine(vectors[id], query)).Take(120).Select(id => (long)id)];
        List<RowKey> found = index.SearchTopK(query, 120);

        Assert.Equal(120, found.Count);
        Assert.True(found.Count(key => exact.Contains(key.AsInt64())) >= 114, "graph recall@120 below 95%");
    }

    [Theory]
    [InlineData("HNSW", "cosine")]
    [InlineData("HNSW", "euclidean")]
    [InlineData("FLAT", "cosine")]
    [InlineData("FLAT", "euclidean")]
    public void RankNearest_OrdersTheGivenRowsByExactDistance(string indexType, string metric)
    {
        float[][] vectors = [.. Enumerable.Range(0, 601).Select(Vector)];
        IVectorIndex index = indexType == "HNSW" ? new HNSWIndex { Metric = metric } : new FlatVectorIndex { Metric = metric };
        for (int id = 1; id <= 600; id++)
        {
            index.Insert((RowKey)(long)id, vectors[id]);
        }

        index.Delete([(RowKey)9L]);
        float[] query = Vector(31);
        Func<float[], float[], double> distance = metric == "cosine" ? Cosine : Euclidean;

        // Every third row, one deleted row and one row the index never held.
        List<RowKey> rows = [.. Enumerable.Range(1, 600).Where(id => id % 3 == 0).Select(id => (RowKey)(long)id), (RowKey)9_999L];
        long[] expected = [.. rows.Select(key => key.AsInt64()).Where(id => id is <= 600 and not 9).OrderBy(id => distance(vectors[id], query)).Take(25)];

        var nearest = new List<RowKey>();
        ((IRowRankingVectorIndex)index).RankNearest(query, rows, 25, nearest);
        Assert.Equal(expected, nearest.Select(key => key.AsInt64()));

        // Asking for more rows than there are returns them all; asking for none returns none.
        nearest.Clear();
        ((IRowRankingVectorIndex)index).RankNearest(query, rows, 10_000, nearest);
        Assert.Equal(199, nearest.Count);
        nearest.Clear();
        ((IRowRankingVectorIndex)index).RankNearest(query, rows, 0, nearest);
        Assert.Empty(nearest);
    }

    private static HNSWIndex Build(out float[][] vectors)
    {
        vectors = [.. Enumerable.Range(0, Vectors + 1).Select(Vector)];
        var index = new HNSWIndex();
        for (int id = 1; id <= Vectors; id++)
        {
            index.Insert((RowKey)(long)id, vectors[id]);
        }

        return index;
    }

    private static float[] Vector(int seed)
    {
        var random = new Random(seed * 7919 + 1);
        var vector = new float[Dimensions];
        for (int i = 0; i < vector.Length; i++)
        {
            vector[i] = (float)(random.NextDouble() * 2 - 1);
        }

        return vector;
    }

    private static double Cosine(float[] left, float[] right)
    {
        double dot = 0, leftNorm = 0, rightNorm = 0;
        for (int i = 0; i < left.Length; i++)
        {
            dot += (double)left[i] * right[i];
            leftNorm += (double)left[i] * left[i];
            rightNorm += (double)right[i] * right[i];
        }

        return 1d - dot / Math.Sqrt(leftNorm * rightNorm);
    }

    private static double Euclidean(float[] left, float[] right)
    {
        double sum = 0;
        for (int i = 0; i < left.Length; i++)
        {
            double difference = (double)left[i] - right[i];
            sum += difference * difference;
        }

        return Math.Sqrt(sum);
    }
}
