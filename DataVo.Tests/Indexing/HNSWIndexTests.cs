using DataVo.Core.Indexing.HNSW;
using System.Diagnostics;

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
        Assert.All(index.Layers, level =>
        {
            int maxNeighbors = level.Key == 0 ? Math.Max(2, index.M * 2) : index.M;
            Assert.All(level.Value.Values, neighbors => Assert.True(neighbors.Count <= maxNeighbors));
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
            EnableAdaptiveEfConstruction = false,
            AdaptiveEfConstructionMultiplier = 2.1d,
            EnableInsertionCandidateExpansion = false,
            InsertionCandidateExpansionFactor = 1.8d,
            EnableAdaptiveInsertionCandidateExpansion = false,
            AdaptiveInsertionExpansionMinFactor = 1.1d,
            AdaptiveInsertionExpansionMaxFactor = 2.2d,
            EnableInsertionNeighborhoodPruning = true,
            InsertionNeighborhoodPruningThreshold = 0.92d,
            InsertionNeighborhoodPruneHops = 2,
            EfSearch = 30,
            EnableDiversityHeuristic = false,
            EnableAdaptiveEfSearch = false,
            AdaptiveEfSearchMultiplier = 2.25d
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
        Assert.False(loaded.EnableAdaptiveEfConstruction);
        Assert.Equal(2.1d, loaded.AdaptiveEfConstructionMultiplier);
        Assert.False(loaded.EnableInsertionCandidateExpansion);
        Assert.Equal(1.8d, loaded.InsertionCandidateExpansionFactor);
        Assert.False(loaded.EnableAdaptiveInsertionCandidateExpansion);
        Assert.Equal(1.1d, loaded.AdaptiveInsertionExpansionMinFactor);
        Assert.Equal(2.2d, loaded.AdaptiveInsertionExpansionMaxFactor);
        Assert.True(loaded.EnableInsertionNeighborhoodPruning);
        Assert.Equal(0.92d, loaded.InsertionNeighborhoodPruningThreshold);
        Assert.Equal(2, loaded.InsertionNeighborhoodPruneHops);
        Assert.Equal(30, loaded.EfSearch);
        Assert.False(loaded.EnableDiversityHeuristic);
        Assert.False(loaded.EnableAdaptiveEfSearch);
        Assert.Equal(2.25d, loaded.AdaptiveEfSearchMultiplier);
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
        const int dimension = 12;
        const int vectors = 280;
        const int queries = 18;
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

    [Fact]
    public void Benchmark_RecallTrend_ImprovesWithEfSearchAndM_Matrix()
    {
        const int dimension = 10;
        const int vectors = 220;
        const int queries = 14;
        const int topK = 10;

        var dataset = BuildDataset(seed: 20260324, vectors, dimension);
        var querySet = BuildQueries(seed: 20260325, queries, dimension);

        double recallLowEf = EvaluateRecallAtK(dataset, querySet, topK, m: 8, efConstruction: 64, efSearch: 16);
        double recallMidEf = EvaluateRecallAtK(dataset, querySet, topK, m: 8, efConstruction: 64, efSearch: 48);
        double recallHighEf = EvaluateRecallAtK(dataset, querySet, topK, m: 8, efConstruction: 64, efSearch: 96);

        Assert.True(recallMidEf >= recallLowEf - 1e-6, $"Expected mid efSearch recall >= low recall. low={recallLowEf:F3}, mid={recallMidEf:F3}");
        Assert.True(recallHighEf >= recallMidEf - 1e-6, $"Expected high efSearch recall >= mid recall. mid={recallMidEf:F3}, high={recallHighEf:F3}");

        double recallLowM = EvaluateRecallAtK(dataset, querySet, topK, m: 8, efConstruction: 96, efSearch: 96);
        double recallHighM = EvaluateRecallAtK(dataset, querySet, topK, m: 16, efConstruction: 96, efSearch: 96);

        Assert.True(recallHighM >= recallLowM - 1e-6, $"Expected higher M not to reduce recall. m8={recallLowM:F3}, m16={recallHighM:F3}");
    }

    [Fact]
    public void Benchmark_RecallTrend_ImprovesWithInsertionCandidateExpansion()
    {
        const int dimension = 10;
        const int vectors = 220;
        const int queries = 14;
        const int topK = 10;

        var dataset = BuildDataset(seed: 20260331, vectors, dimension);
        var querySet = BuildQueries(seed: 20260401, queries, dimension);

        double recallBaseline = EvaluateRecallAtK(
            dataset,
            querySet,
            topK,
            m: 12,
            efConstruction: 72,
            efSearch: 72,
            enableInsertionCandidateExpansion: false,
            insertionCandidateExpansionFactor: 1.0d);

        double recallExpanded = EvaluateRecallAtK(
            dataset,
            querySet,
            topK,
            m: 12,
            efConstruction: 72,
            efSearch: 72,
            enableInsertionCandidateExpansion: true,
            insertionCandidateExpansionFactor: 1.75d);

        Assert.True(
            recallExpanded >= recallBaseline - 0.02d,
            $"Expected insertion candidate expansion to maintain or improve recall trend. baseline={recallBaseline:F3}, expanded={recallExpanded:F3}");
    }

    [Fact]
    public void Benchmark_RecallTrend_AdaptiveInsertionExpansionPolicy_IsStable()
    {
        const int dimension = 10;
        const int vectors = 210;
        const int queries = 14;
        const int topK = 10;

        var dataset = BuildDataset(seed: 20260402, vectors, dimension);
        var querySet = BuildQueries(seed: 20260403, queries, dimension);

        double recallFixed = EvaluateRecallAtK(
            dataset,
            querySet,
            topK,
            m: 12,
            efConstruction: 72,
            efSearch: 72,
            enableInsertionCandidateExpansion: true,
            insertionCandidateExpansionFactor: 1.5d,
            enableAdaptiveInsertionCandidateExpansion: false);

        double recallAdaptive = EvaluateRecallAtK(
            dataset,
            querySet,
            topK,
            m: 12,
            efConstruction: 72,
            efSearch: 72,
            enableInsertionCandidateExpansion: true,
            insertionCandidateExpansionFactor: 1.5d,
            enableAdaptiveInsertionCandidateExpansion: true,
            adaptiveInsertionExpansionMinFactor: 1.0d,
            adaptiveInsertionExpansionMaxFactor: 2.4d);

        Assert.True(
            recallAdaptive >= recallFixed - 0.03d,
            $"Expected adaptive insertion expansion policy to remain stable. fixed={recallFixed:F3}, adaptive={recallAdaptive:F3}");
    }

    [Fact]
    public void Benchmark_RecallStability_UnderInsertDeleteChurn_RemainsBounded()
    {
        const int dimension = 10;
        const int vectors = 220;
        const int queries = 14;
        const int topK = 10;
        const int churnCycles = 4;
        const int churnBatch = 24;

        var random = new Random(20260326);
        var dataset = BuildDataset(seed: 20260326, vectors, dimension);
        var querySet = BuildQueries(seed: 20260327, queries, dimension);

        var index = new HNSWIndex
        {
            Metric = "cosine",
            M = 12,
            EfConstruction = 96,
            EnableAdaptiveEfConstruction = true,
            AdaptiveEfConstructionMultiplier = 1.5d,
            EfSearch = 96,
            EnableAdaptiveEfSearch = true,
            EnableDiversityHeuristic = true,
            EnableDeleteGraphRepair = true
        };

        foreach (var (rowId, vector) in dataset)
        {
            index.Insert(rowId, vector);
        }

        long nextId = vectors + 1;
        var recalls = new List<double>
        {
            EvaluateRecallAtK(index, querySet, topK)
        };

        for (int cycle = 0; cycle < churnCycles; cycle++)
        {
            List<long> removable = index.Entries.Keys
                .OrderBy(_ => random.Next())
                .Take(churnBatch)
                .ToList();

            index.Delete(removable);

            for (int i = 0; i < churnBatch; i++)
            {
                float[] vector = new float[dimension];
                for (int d = 0; d < dimension; d++)
                {
                    vector[d] = (float)random.NextDouble();
                }

                index.Insert(nextId++, vector);
            }

            Assert.Equal(vectors, index.Count);
            recalls.Add(EvaluateRecallAtK(index, querySet, topK));
        }

        double minRecall = recalls.Min();
        double maxRecall = recalls.Max();
        double drift = maxRecall - minRecall;

        Assert.True(minRecall >= 0.45d, $"Expected churn min recall@10 >= 0.45, got {minRecall:F3}");
        Assert.True(drift <= 0.35d, $"Expected churn recall drift <= 0.35, got {drift:F3}");
    }

    [Fact]
    public void Benchmark_ChurnMatrix_RecallAndLatencyRemainStable()
    {
        const int dimension = 12;
        const int vectors = 260;
        const int queries = 16;
        const int topK = 10;
        const int churnCycles = 5;

        double[] churnRatios = [0.05d, 0.10d, 0.20d];
        var baseDataset = BuildDataset(seed: 20260328, vectors, dimension);
        var querySet = BuildQueries(seed: 20260329, queries, dimension);

        var ratioToAvgRecall = new Dictionary<double, double>();

        for (int ratioIndex = 0; ratioIndex < churnRatios.Length; ratioIndex++)
        {
            double churnRatio = churnRatios[ratioIndex];
            int churnBatch = Math.Max(1, (int)Math.Round(vectors * churnRatio));

            var random = new Random(20260330 + ratioIndex);
            var index = new HNSWIndex
            {
                Metric = "cosine",
                M = 12,
                EfConstruction = 96,
                EnableAdaptiveEfConstruction = true,
                AdaptiveEfConstructionMultiplier = 1.5d,
                EfSearch = 96,
                EnableAdaptiveEfSearch = true,
                EnableDiversityHeuristic = true,
                EnableDeleteGraphRepair = true
            };

            foreach (var (rowId, vector) in baseDataset)
            {
                index.Insert(rowId, vector);
            }

            long nextId = vectors + 1;
            var snapshots = new List<QueryQualitySnapshot>
            {
                EvaluateRecallAndLatencyAtK(index, querySet, topK)
            };

            for (int cycle = 0; cycle < churnCycles; cycle++)
            {
                List<long> removable = index.Entries.Keys
                    .OrderBy(_ => random.Next())
                    .Take(churnBatch)
                    .ToList();

                index.Delete(removable);

                for (int i = 0; i < churnBatch; i++)
                {
                    float[] vector = new float[dimension];
                    for (int d = 0; d < dimension; d++)
                    {
                        vector[d] = (float)random.NextDouble();
                    }

                    index.Insert(nextId++, vector);
                }

                Assert.Equal(vectors, index.Count);
                snapshots.Add(EvaluateRecallAndLatencyAtK(index, querySet, topK));
            }

            double minRecall = snapshots.Min(snapshot => snapshot.AvgRecall);
            double maxRecall = snapshots.Max(snapshot => snapshot.AvgRecall);
            double avgRecall = snapshots.Average(snapshot => snapshot.AvgRecall);
            double drift = maxRecall - minRecall;

            double baselineP95 = snapshots.First().P95LatencyMs;
            double maxP95 = snapshots.Max(snapshot => snapshot.P95LatencyMs);
            double p95MultiplierCap = Math.Max(12.0d, baselineP95 * 8.0d + 1.0d);

            Assert.True(minRecall >= 0.40d, $"Expected min recall@10 >= 0.40 for churnRatio={churnRatio:F2}, got {minRecall:F3}");
            Assert.True(drift <= 0.40d, $"Expected recall drift <= 0.40 for churnRatio={churnRatio:F2}, got {drift:F3}");
            Assert.True(maxP95 <= p95MultiplierCap, $"Expected p95 latency bounded for churnRatio={churnRatio:F2}. baseline={baselineP95:F3}ms, max={maxP95:F3}ms");

            ratioToAvgRecall[churnRatio] = avgRecall;
        }

        Assert.True(ratioToAvgRecall[0.05d] + 1e-6 >= ratioToAvgRecall[0.20d] - 0.15d,
            $"Expected low-churn average recall to stay close to high-churn. low={ratioToAvgRecall[0.05d]:F3}, high={ratioToAvgRecall[0.20d]:F3}");
    }

    [Fact]
    public void Benchmark_ChurnSoak_MultiRunVariance_IsBounded()
    {
        const int runs = 2;
        const int dimension = 10;
        const int vectors = 200;
        const int queries = 12;
        const int topK = 10;
        const int churnCycles = 4;

        double[] churnRatios = [0.10d, 0.20d];
        var recallByRatio = new Dictionary<double, List<double>>();
        var p95ByRatio = new Dictionary<double, List<double>>();

        foreach (double ratio in churnRatios)
        {
            recallByRatio[ratio] = [];
            p95ByRatio[ratio] = [];
        }

        for (int run = 0; run < runs; run++)
        {
            var dataset = BuildDataset(seed: 20260410 + run, vectors, dimension);
            var querySet = BuildQueries(seed: 20260420 + run, queries, dimension);

            foreach (double churnRatio in churnRatios)
            {
                ChurnExperimentResult result = RunChurnExperiment(
                    dataset,
                    querySet,
                    topK,
                    churnRatio,
                    churnCycles,
                    seed: 20260430 + (run * 10) + (int)(churnRatio * 100));

                recallByRatio[churnRatio].Add(result.AvgRecall);
                p95ByRatio[churnRatio].Add(result.P95LatencyMs);
            }
        }

        foreach (double ratio in churnRatios)
        {
            List<double> recalls = recallByRatio[ratio];
            List<double> p95s = p95ByRatio[ratio];

            double meanRecall = recalls.Average();
            double recallStdDev = StdDev(recalls);
            double meanP95 = p95s.Average();
            double p95StdDev = StdDev(p95s);

            Assert.True(meanRecall >= 0.38d, $"Expected mean recall >= 0.38 for churn ratio {ratio:F2}, got {meanRecall:F3}");
            Assert.True(recallStdDev <= 0.10d, $"Expected recall stddev <= 0.10 for churn ratio {ratio:F2}, got {recallStdDev:F3}");
            Assert.True(p95StdDev <= Math.Max(8.0d, meanP95 * 1.25d), $"Expected bounded p95 variance for churn ratio {ratio:F2}. mean={meanP95:F3}ms stddev={p95StdDev:F3}ms");
        }
    }

    private static List<(long RowId, float[] Vector)> BuildDataset(int seed, int vectors, int dimension)
    {
        var random = new Random(seed);
        var dataset = new List<(long RowId, float[] Vector)>(vectors);

        for (int id = 1; id <= vectors; id++)
        {
            float[] vector = new float[dimension];
            for (int i = 0; i < dimension; i++)
            {
                vector[i] = (float)random.NextDouble();
            }

            dataset.Add((id, vector));
        }

        return dataset;
    }

    private static List<float[]> BuildQueries(int seed, int queries, int dimension)
    {
        var random = new Random(seed);
        var querySet = new List<float[]>(queries);

        for (int q = 0; q < queries; q++)
        {
            float[] query = new float[dimension];
            for (int i = 0; i < dimension; i++)
            {
                query[i] = (float)random.NextDouble();
            }

            querySet.Add(query);
        }

        return querySet;
    }

    private static double EvaluateRecallAtK(
        List<(long RowId, float[] Vector)> dataset,
        List<float[]> querySet,
        int topK,
        int m,
        int efConstruction,
        int efSearch,
        bool enableInsertionCandidateExpansion = true,
        double insertionCandidateExpansionFactor = 1.5d,
        bool enableAdaptiveInsertionCandidateExpansion = true,
        double adaptiveInsertionExpansionMinFactor = 1.0d,
        double adaptiveInsertionExpansionMaxFactor = 2.5d)
    {
        var index = new HNSWIndex
        {
            Metric = "cosine",
            M = m,
            EfConstruction = efConstruction,
            EnableInsertionCandidateExpansion = enableInsertionCandidateExpansion,
            InsertionCandidateExpansionFactor = insertionCandidateExpansionFactor,
            EnableAdaptiveInsertionCandidateExpansion = enableAdaptiveInsertionCandidateExpansion,
            AdaptiveInsertionExpansionMinFactor = adaptiveInsertionExpansionMinFactor,
            AdaptiveInsertionExpansionMaxFactor = adaptiveInsertionExpansionMaxFactor,
            EfSearch = efSearch,
            EnableDiversityHeuristic = true,
            EnableDeleteGraphRepair = true
        };

        foreach (var (rowId, vector) in dataset)
        {
            index.Insert(rowId, vector);
        }

        double totalRecall = 0d;
        foreach (float[] query in querySet)
        {
            List<long> ann = index.SearchTopK(query, topK);
            List<long> exact = dataset
                .Select(entry => (entry.RowId, DistanceCosine(query, entry.Vector)))
                .OrderBy(item => item.Item2)
                .ThenBy(item => item.RowId)
                .Take(topK)
                .Select(item => item.RowId)
                .ToList();

            int overlap = ann.Intersect(exact).Count();
            totalRecall += (double)overlap / topK;
        }

        return totalRecall / querySet.Count;
    }

    private static double EvaluateRecallAtK(HNSWIndex index, List<float[]> querySet, int topK)
    {
        double totalRecall = 0d;
        foreach (float[] query in querySet)
        {
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

        return totalRecall / querySet.Count;
    }

    private static QueryQualitySnapshot EvaluateRecallAndLatencyAtK(HNSWIndex index, List<float[]> querySet, int topK)
    {
        double totalRecall = 0d;
        var latencies = new List<double>(querySet.Count);

        foreach (float[] query in querySet)
        {
            var stopwatch = Stopwatch.StartNew();
            List<long> ann = index.SearchTopK(query, topK);
            stopwatch.Stop();
            latencies.Add(stopwatch.Elapsed.TotalMilliseconds);

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

        double avgRecall = totalRecall / querySet.Count;
        double avgLatency = latencies.Average();
        double p95Latency = latencies
            .OrderBy(value => value)
            .ElementAt(Math.Min(latencies.Count - 1, (int)Math.Ceiling(latencies.Count * 0.95d) - 1));

        return new QueryQualitySnapshot(avgRecall, avgLatency, p95Latency);
    }

    private static ChurnExperimentResult RunChurnExperiment(
        List<(long RowId, float[] Vector)> dataset,
        List<float[]> querySet,
        int topK,
        double churnRatio,
        int churnCycles,
        int seed)
    {
        var random = new Random(seed);
        int vectors = dataset.Count;
        int dimension = dataset[0].Vector.Length;
        int churnBatch = Math.Max(1, (int)Math.Round(vectors * churnRatio));

        var index = new HNSWIndex
        {
            Metric = "cosine",
            M = 12,
            EfConstruction = 96,
            EnableAdaptiveEfConstruction = true,
            AdaptiveEfConstructionMultiplier = 1.5d,
            EnableInsertionCandidateExpansion = true,
            InsertionCandidateExpansionFactor = 1.5d,
            EnableAdaptiveInsertionCandidateExpansion = true,
            AdaptiveInsertionExpansionMinFactor = 1.0d,
            AdaptiveInsertionExpansionMaxFactor = 2.5d,
            EnableInsertionNeighborhoodPruning = true,
            InsertionNeighborhoodPruningThreshold = 0.85d,
            InsertionNeighborhoodPruneHops = 1,
            EfSearch = 96,
            EnableAdaptiveEfSearch = true,
            EnableDiversityHeuristic = true,
            EnableDeleteGraphRepair = true
        };

        foreach (var (rowId, vector) in dataset)
        {
            index.Insert(rowId, vector);
        }

        long nextId = vectors + 1;
        var snapshots = new List<QueryQualitySnapshot>
        {
            EvaluateRecallAndLatencyAtK(index, querySet, topK)
        };

        for (int cycle = 0; cycle < churnCycles; cycle++)
        {
            List<long> removable = index.Entries.Keys
                .OrderBy(_ => random.Next())
                .Take(churnBatch)
                .ToList();

            index.Delete(removable);

            for (int i = 0; i < churnBatch; i++)
            {
                float[] vector = new float[dimension];
                for (int d = 0; d < dimension; d++)
                {
                    vector[d] = (float)random.NextDouble();
                }

                index.Insert(nextId++, vector);
            }

            snapshots.Add(EvaluateRecallAndLatencyAtK(index, querySet, topK));
        }

        return new ChurnExperimentResult(
            snapshots.Average(snapshot => snapshot.AvgRecall),
            snapshots.Average(snapshot => snapshot.AvgLatencyMs),
            snapshots.Max(snapshot => snapshot.P95LatencyMs));
    }

    private static double StdDev(List<double> values)
    {
        if (values.Count <= 1)
        {
            return 0d;
        }

        double mean = values.Average();
        double variance = values.Sum(value => Math.Pow(value - mean, 2)) / values.Count;
        return Math.Sqrt(variance);
    }

    private readonly record struct QueryQualitySnapshot(double AvgRecall, double AvgLatencyMs, double P95LatencyMs);
    private readonly record struct ChurnExperimentResult(double AvgRecall, double AvgLatencyMs, double P95LatencyMs);

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
