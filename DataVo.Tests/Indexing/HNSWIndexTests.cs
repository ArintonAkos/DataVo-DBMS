using DataVo.Core.Indexing;
using DataVo.Core.Indexing.HNSW;
using System.Diagnostics;
using System.Globalization;

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
    public void Hnsw_AdvertisesSpanInsertAndReservationCapabilities()
    {
        var index = new HNSWIndex();

        Assert.IsAssignableFrom<ISpanVectorIndex>(index);
        Assert.IsAssignableFrom<IReservableVectorIndex>(index);
    }

    [Fact]
    public void Factory_AppliesMaxAdaptiveEfConstruction()
    {
        var factory = new HNSWIndexFactory();

        var index = Assert.IsType<HNSWIndex>(factory.CreateIndex(
            "idx",
            "emb",
            new Dictionary<string, object> { ["maxAdaptiveEfConstruction"] = 128 }));

        Assert.Equal(128, index.MaxAdaptiveEfConstruction);
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

        List<long> nearestToX = index.SearchTopK([0.99f, 0.01f, 0f], 2);
        Assert.NotEmpty(nearestToX);
        Assert.Equal(1, nearestToX[0]);

        List<long> nearestToY = index.SearchTopK([0f, 1f, 0f], 2);
        Assert.NotEmpty(nearestToY);
        Assert.Equal(3, nearestToY[0]);
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
    public void Insert_RejectsNonFiniteVectorValues()
    {
        var index = new HNSWIndex { Metric = "cosine" };

        Assert.Throws<ArgumentException>(() => index.Insert(1, new float[] { 1f, float.NaN, 0f }));
        Assert.Throws<ArgumentException>(() => index.Insert(2, new float[] { 1f, float.PositiveInfinity, 0f }));
    }

    [Fact]
    public void SearchTopK_RejectsNonFiniteQueryValues()
    {
        var index = new HNSWIndex { Metric = "cosine" };
        index.Insert(1, new float[] { 1f, 0f, 0f });

        Assert.Throws<ArgumentException>(() => index.SearchTopK(new float[] { 1f, float.NaN, 0f }, 1));
        Assert.Throws<ArgumentException>(() => index.SearchTopK(new float[] { 1f, float.NegativeInfinity, 0f }, 1));
    }

    [Theory]
    [InlineData("cosine")]
    [InlineData("euclidean")]
    public void SearchTopK_ExactMode_MatchesScalarDistanceOrdering(string metric)
    {
        const int dimension = 257; // Non-multiple of SIMD width exercises scalar tail handling.
        const int vectors = 64;
        var rng = new Random(20260326);

        var index = new HNSWIndex
        {
            Metric = metric,
            EfSearch = 64,
            EfConstruction = 64,
            M = 12
        };

        var data = new Dictionary<long, float[]>();
        for (int id = 1; id <= vectors; id++)
        {
            float[] vector = new float[dimension];
            for (int i = 0; i < dimension; i++)
            {
                vector[i] = (float)rng.NextDouble();
            }

            data[id] = vector;
            index.Insert(id, vector);
        }

        float[] query = new float[dimension];
        for (int i = 0; i < dimension; i++)
        {
            query[i] = (float)rng.NextDouble();
        }

        List<long> expected = data
            .Select(entry => (entry.Key, Distance: metric == "cosine"
                ? DistanceCosine(query, entry.Value)
                : DistanceEuclidean(query, entry.Value)))
            .OrderBy(item => item.Distance)
            .ThenBy(item => item.Key)
            .Select(item => item.Key)
            .ToList();

        // topK >= count forces exact path and validates kernel parity deterministically.
        List<long> actual = index.SearchTopK(query, vectors);

        Assert.Equal(expected, actual);
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

        List<long> nearest = loaded.SearchTopK([0.95f, 0.05f, 0f], 1);
        Assert.Single(nearest);
    }

    [Fact]
    public void Delete_RemovesNodeAndKeepsNearestNeighborQueriesStable()
    {
        var index = new HNSWIndex
        {
            Metric = "euclidean",
            M = 8,
            EnableDeleteGraphRepair = true,
            EnableDiversityHeuristic = false
        };

        index.Insert(1, [0f, 0f]);
        index.Insert(2, [1f, 0f]);
        index.Insert(3, [2f, 0f]);

        index.Delete([2]);

        Assert.Equal(2, index.Count);

        List<long> nearestLeft = index.SearchTopK([0.05f, 0f], 1);
        List<long> nearestRight = index.SearchTopK([1.95f, 0f], 1);

        Assert.Single(nearestLeft);
        Assert.Single(nearestRight);
        Assert.Equal(1, nearestLeft[0]);
        Assert.Equal(3, nearestRight[0]);
    }

    [Fact]
    public void Benchmark_RecallAt10_AgainstExactBaseline_IsReasonable()
    {
        if (!ShouldRunHnswPerformanceBenchmarks())
        {
            return;
        }

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

        var exactDataset = new List<(long RowId, float[] Vector)>(vectors);
        for (int id = 1; id <= vectors; id++)
        {
            float[] vector = new float[dimension];
            for (int i = 0; i < dimension; i++)
            {
                vector[i] = (float)random.NextDouble();
            }

            index.Insert(id, vector);
            exactDataset.Add((id, vector));
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
            List<long> exact = exactDataset
                .Select(entry => (entry.RowId, DistanceCosine(query, entry.Vector)))
                .OrderBy(item => item.Item2)
                .ThenBy(item => item.RowId)
                .Take(topK)
                .Select(item => item.RowId)
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
        if (!ShouldRunHnswPerformanceBenchmarks())
        {
            return;
        }

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
        if (!ShouldRunHnswPerformanceBenchmarks())
        {
            return;
        }

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
        if (!ShouldRunHnswPerformanceBenchmarks())
        {
            return;
        }

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
        if (!ShouldRunHnswPerformanceBenchmarks())
        {
            return;
        }

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
        var activeRows = new Dictionary<long, float[]>(vectors);
        foreach (var (rowId, vector) in dataset)
        {
            activeRows[rowId] = vector;
        }
        var recalls = new List<double>
        {
            EvaluateRecallAtK(index, activeRows, querySet, topK)
        };

        for (int cycle = 0; cycle < churnCycles; cycle++)
        {
            List<long> removable = activeRows.Keys
                .OrderBy(_ => random.Next())
                .Take(churnBatch)
                .ToList();

            index.Delete(removable);
            for (int i = 0; i < removable.Count; i++)
            {
                activeRows.Remove(removable[i]);
            }

            for (int i = 0; i < churnBatch; i++)
            {
                float[] vector = new float[dimension];
                for (int d = 0; d < dimension; d++)
                {
                    vector[d] = (float)random.NextDouble();
                }

                long insertedId = nextId++;
                index.Insert(insertedId, vector);
                activeRows[insertedId] = vector;
            }

            Assert.Equal(vectors, index.Count);
            recalls.Add(EvaluateRecallAtK(index, activeRows, querySet, topK));
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
        if (!ShouldRunHnswPerformanceBenchmarks())
        {
            return;
        }

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
            var activeRows = new Dictionary<long, float[]>(vectors);
            foreach (var (rowId, vector) in baseDataset)
            {
                activeRows[rowId] = vector;
            }
            var snapshots = new List<QueryQualitySnapshot>
            {
                EvaluateRecallAndLatencyAtK(index, activeRows, querySet, topK)
            };

            for (int cycle = 0; cycle < churnCycles; cycle++)
            {
                List<long> removable = activeRows.Keys
                    .OrderBy(_ => random.Next())
                    .Take(churnBatch)
                    .ToList();

                index.Delete(removable);
                for (int i = 0; i < removable.Count; i++)
                {
                    activeRows.Remove(removable[i]);
                }

                for (int i = 0; i < churnBatch; i++)
                {
                    float[] vector = new float[dimension];
                    for (int d = 0; d < dimension; d++)
                    {
                        vector[d] = (float)random.NextDouble();
                    }

                    long insertedId = nextId++;
                    index.Insert(insertedId, vector);
                    activeRows[insertedId] = vector;
                }

                Assert.Equal(vectors, index.Count);
                snapshots.Add(EvaluateRecallAndLatencyAtK(index, activeRows, querySet, topK));
            }

            double minRecall = snapshots.Min(snapshot => snapshot.AvgRecall);
            double maxRecall = snapshots.Max(snapshot => snapshot.AvgRecall);
            double avgRecall = snapshots.Average(snapshot => snapshot.AvgRecall);
            double drift = maxRecall - minRecall;

            double baselineP95 = snapshots.First().P95LatencyMs;
            double maxP95 = snapshots.Max(snapshot => snapshot.P95LatencyMs);
            // Baseline p95 can be sub-millisecond on warm runs, so use a wider floor to avoid host-noise flakes.
            double p95MultiplierCap = Math.Max(22.0d, baselineP95 * 16.0d + 3.0d);

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
        if (!ShouldRunHnswPerformanceBenchmarks())
        {
            return;
        }

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

    [Fact]
    [Trait("Category", "Performance")]
    [Trait("Profile", "VeryLargeDataset")]
    public void Benchmark_Build_1M_Vectors_Profile()
    {
        if (!ShouldRunHnswPerformanceBenchmarks())
        {
            return;
        }

        bool runLong = ShouldRunLongBuildCheckpoints();
        int[] scalingSteps = runLong
            ? [1_000, 5_000, 10_000, 25_000, 50_000, 100_000, 1_000_000]
            : [1_000, 5_000, 10_000];

        if (!runLong)
        {
            Console.WriteLine("[CHECKPOINT][BuildScaling][Mode] fast (set DATAVO_RUN_LONG_PERF=1 to include 25k/50k/100k)");
        }

        const int dimensions = 32;
        const int m = 16;
        const int efConstruction = 100;

        for (int stepIndex = 0; stepIndex < scalingSteps.Length; stepIndex++)
        {
            int vectorCount = scalingSteps[stepIndex];
            Console.WriteLine(FormattableString.Invariant($"[CHECKPOINT][BuildScaling][Start] vectors={vectorCount}, dim={dimensions}"));

            var generationStopwatch = Stopwatch.StartNew();
            var vectors = new float[vectorCount * dimensions];
            var ids = new long[vectorCount];

            Parallel.For(0, vectorCount, i =>
            {
                ids[i] = i + 1;
                int offset = i * dimensions;
                for (int d = 0; d < dimensions; d++)
                {
                    vectors[offset + d] = (i + d) * 0.01f;
                }
            });
            generationStopwatch.Stop();

            Console.WriteLine(FormattableString.Invariant($"[CHECKPOINT][BuildScaling][DataReady] vectors={vectorCount}, generationMs={generationStopwatch.Elapsed.TotalMilliseconds:F1}"));

            var index = new HNSWIndex
            {
                Metric = "cosine",
                M = m,
                EfConstruction = efConstruction,
                EfSearch = 64,
                EnableAdaptiveEfConstruction = false,
                EnableAdaptiveEfSearch = false,
                EnableInsertionCandidateExpansion = true,
                EnableAdaptiveInsertionCandidateExpansion = false,
                EnableInsertionNeighborhoodPruning = true,
                EnableDeleteGraphRepair = true,
                EnableDiversityHeuristic = true
            };

            int buildDop = Environment.ProcessorCount;
            string? dopOverride = Environment.GetEnvironmentVariable("DATAVO_HNSW_BUILD_DOP");
            if (!string.IsNullOrWhiteSpace(dopOverride) && int.TryParse(dopOverride, out int parsedDop) && parsedDop > 0)
            {
                buildDop = parsedDop;
            }

            index.Reserve(vectorCount, dimensions);

            long memoryBefore = GC.GetTotalMemory(forceFullCollection: false);
            var insertTotal = Stopwatch.StartNew();
            index.InsertBatchParallel(ids, vectors, dimensions, buildDop);

            insertTotal.Stop();
            long memoryAfter = GC.GetTotalMemory(forceFullCollection: false);
            double memDeltaMb = (memoryAfter - memoryBefore) / (1024d * 1024d);
            double insertsPerSecond = vectorCount / Math.Max(0.001d, insertTotal.Elapsed.TotalMilliseconds / 1000d);

            Console.WriteLine(FormattableString.Invariant(
                $"[CHECKPOINT][BuildScaling][Insert] vectors={vectorCount}, inserted={vectorCount}, totalMs={insertTotal.Elapsed.TotalMilliseconds:F1}, ips={insertsPerSecond:F1}, dop={buildDop}"));

            Console.WriteLine(FormattableString.Invariant(
                $"[PROFILE][BuildScaling][Summary] vectors={vectorCount}, genMs={generationStopwatch.Elapsed.TotalMilliseconds:F1}, insertMs={insertTotal.Elapsed.TotalMilliseconds:F1}, totalMs={(generationStopwatch.Elapsed.TotalMilliseconds + insertTotal.Elapsed.TotalMilliseconds):F1}, count={index.Count}, memDeltaMb={memDeltaMb:F1}"));

            Assert.Equal(vectorCount, index.Count);
            Assert.True(insertTotal.ElapsedMilliseconds < 300_000, $"HNSW build exceeded 5 minutes at vectors={vectorCount}: {insertTotal.ElapsedMilliseconds}ms");
        }
    }

    private static bool ShouldRunLongBuildCheckpoints()
    {
        string? value = Environment.GetEnvironmentVariable("DATAVO_RUN_LONG_PERF");
        return string.Equals(value, "1", StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);
    }

    private static bool ShouldRunHnswPerformanceBenchmarks()
    {
        string? value = Environment.GetEnvironmentVariable("DATAVO_RUN_HNSW_PERF_TESTS");
        return string.Equals(value, "1", StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    [Trait("Category", "Performance")]
    [Trait("Profile", "LargeDataset")]
    public void Benchmark_LargeDataset_RecallLatencyProfile_EfSearchScaling_IsStable()
    {
        if (!ShouldRunHnswPerformanceBenchmarks())
        {
            return;
        }

        const int vectors = 900;
        const int dimension = 48;
        const int queries = 20;
        const int topK = 10;

        var dataset = BuildDataset(seed: 20260440, vectors, dimension);
        var querySet = BuildQueries(seed: 20260441, queries, dimension);
        List<List<long>> exactBaseline = BuildExactTopKBaseline(dataset, querySet, topK);

        (QueryQualitySnapshot low, ProfileBreakdown lowProfile) = EvaluateRecallAndLatencyProfileAtK(
            dataset,
            querySet,
            topK,
            m: 12,
            efConstruction: 128,
            efSearch: 32,
            exactTopKBaseline: exactBaseline);

        (QueryQualitySnapshot mid, ProfileBreakdown midProfile) = EvaluateRecallAndLatencyProfileAtK(
            dataset,
            querySet,
            topK,
            m: 12,
            efConstruction: 128,
            efSearch: 96,
            exactTopKBaseline: exactBaseline);

        (QueryQualitySnapshot high, ProfileBreakdown highProfile) = EvaluateRecallAndLatencyProfileAtK(
            dataset,
            querySet,
            topK,
            m: 12,
            efConstruction: 128,
            efSearch: 160,
            exactTopKBaseline: exactBaseline);

        Console.WriteLine(FormattableString.Invariant($"[PROFILE][EfScaling] buildMs low/mid/high={lowProfile.BuildMs:F1}/{midProfile.BuildMs:F1}/{highProfile.BuildMs:F1}, annMs low/mid/high={lowProfile.AnnTotalMs:F1}/{midProfile.AnnTotalMs:F1}/{highProfile.AnnTotalMs:F1}, exactMs low/mid/high={lowProfile.ExactTotalMs:F1}/{midProfile.ExactTotalMs:F1}/{highProfile.ExactTotalMs:F1}, buildDeltaMb low/mid/high={lowProfile.BuildManagedDeltaMb:F1}/{midProfile.BuildManagedDeltaMb:F1}/{highProfile.BuildManagedDeltaMb:F1}"));

        Assert.True(low.AvgRecall >= 0.35d, $"Expected low efSearch recall floor >= 0.35, got {low.AvgRecall:F3}");
        Assert.True(mid.AvgRecall >= low.AvgRecall - 0.02d, $"Expected mid efSearch recall >= low recall - 0.02. low={low.AvgRecall:F3}, mid={mid.AvgRecall:F3}");
        Assert.True(high.AvgRecall >= mid.AvgRecall - 0.02d, $"Expected high efSearch recall >= mid recall - 0.02. mid={mid.AvgRecall:F3}, high={high.AvgRecall:F3}");

        double maxAllowedHighP95 = Math.Max(80.0d, (low.P95LatencyMs * 12.0d) + 2.0d);
        Assert.True(high.P95LatencyMs <= maxAllowedHighP95,
            $"Expected high efSearch p95 latency bounded. low={low.P95LatencyMs:F3}ms, high={high.P95LatencyMs:F3}ms, cap={maxAllowedHighP95:F3}ms");
    }

    [Fact]
    [Trait("Category", "Performance")]
    [Trait("Profile", "LargeDataset")]
    public void Benchmark_LargeDataset_RecallLatencyProfile_ConnectivityScaling_IsStable()
    {
        if (!ShouldRunHnswPerformanceBenchmarks())
        {
            return;
        }

        const int vectors = 800;
        const int dimension = 64;
        const int queries = 18;
        const int topK = 10;

        var dataset = BuildDataset(seed: 20260442, vectors, dimension);
        var querySet = BuildQueries(seed: 20260443, queries, dimension);
        List<List<long>> exactBaseline = BuildExactTopKBaseline(dataset, querySet, topK);

        (QueryQualitySnapshot lowM, ProfileBreakdown lowProfile) = EvaluateRecallAndLatencyProfileAtK(
            dataset,
            querySet,
            topK,
            m: 8,
            efConstruction: 128,
            efSearch: 128,
            exactTopKBaseline: exactBaseline);

        (QueryQualitySnapshot highM, ProfileBreakdown highProfile) = EvaluateRecallAndLatencyProfileAtK(
            dataset,
            querySet,
            topK,
            m: 16,
            efConstruction: 128,
            efSearch: 128,
            exactTopKBaseline: exactBaseline);

        Console.WriteLine(FormattableString.Invariant($"[PROFILE][Connectivity] buildMs m8/m16={lowProfile.BuildMs:F1}/{highProfile.BuildMs:F1}, annMs m8/m16={lowProfile.AnnTotalMs:F1}/{highProfile.AnnTotalMs:F1}, exactMs m8/m16={lowProfile.ExactTotalMs:F1}/{highProfile.ExactTotalMs:F1}, buildDeltaMb m8/m16={lowProfile.BuildManagedDeltaMb:F1}/{highProfile.BuildManagedDeltaMb:F1}"));

        Assert.True(highM.AvgRecall >= lowM.AvgRecall - 0.03d,
            $"Expected higher connectivity to preserve/improve recall trend. m8={lowM.AvgRecall:F3}, m16={highM.AvgRecall:F3}");

        double maxAllowedP95 = Math.Max(90.0d, (lowM.P95LatencyMs * 4.0d) + 3.0d);
        Assert.True(highM.P95LatencyMs <= maxAllowedP95,
            $"Expected higher connectivity p95 latency bounded. m8={lowM.P95LatencyMs:F3}ms, m16={highM.P95LatencyMs:F3}ms, cap={maxAllowedP95:F3}ms");
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

    private static double EvaluateRecallAtK(HNSWIndex index, IReadOnlyDictionary<long, float[]> dataset, List<float[]> querySet, int topK)
    {
        double totalRecall = 0d;
        foreach (float[] query in querySet)
        {
            List<long> ann = index.SearchTopK(query, topK);
            List<long> exact = dataset
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

    private static QueryQualitySnapshot EvaluateRecallAndLatencyAtK(HNSWIndex index, IReadOnlyDictionary<long, float[]> dataset, List<float[]> querySet, int topK)
    {
        double totalRecall = 0d;
        var latencies = new List<double>(querySet.Count);

        foreach (float[] query in querySet)
        {
            var stopwatch = Stopwatch.StartNew();
            List<long> ann = index.SearchTopK(query, topK);
            stopwatch.Stop();
            latencies.Add(stopwatch.Elapsed.TotalMilliseconds);

            List<long> exact = dataset
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

    private static QueryQualitySnapshot EvaluateRecallAndLatencyAtK(
        List<(long RowId, float[] Vector)> dataset,
        List<float[]> querySet,
        int topK,
        int m,
        int efConstruction,
        int efSearch)
    {
        var index = new HNSWIndex
        {
            Metric = "cosine",
            M = m,
            EfConstruction = efConstruction,
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
            EfSearch = efSearch,
            EnableAdaptiveEfSearch = true,
            EnableDiversityHeuristic = true,
            EnableDeleteGraphRepair = true
        };

        foreach (var (rowId, vector) in dataset)
        {
            index.Insert(rowId, vector);
        }

        var exactDataset = dataset.ToDictionary(item => item.RowId, item => item.Vector);
        return EvaluateRecallAndLatencyAtK(index, exactDataset, querySet, topK);
    }

    private static (QueryQualitySnapshot Snapshot, ProfileBreakdown Profile) EvaluateRecallAndLatencyProfileAtK(
        List<(long RowId, float[] Vector)> dataset,
        List<float[]> querySet,
        int topK,
        int m,
        int efConstruction,
        int efSearch,
        List<List<long>>? exactTopKBaseline = null)
    {
        var buildStopwatch = Stopwatch.StartNew();
        long memoryBeforeBuildBytes = GC.GetTotalMemory(forceFullCollection: false);
        var index = new HNSWIndex
        {
            Metric = "cosine",
            M = m,
            EfConstruction = efConstruction,
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
            EfSearch = efSearch,
            EnableAdaptiveEfSearch = true,
            EnableDiversityHeuristic = true,
            EnableDeleteGraphRepair = true
        };

        foreach (var (rowId, vector) in dataset)
        {
            index.Insert(rowId, vector);
        }

        long memoryAfterBuildBytes = GC.GetTotalMemory(forceFullCollection: false);
        buildStopwatch.Stop();

        double totalRecall = 0d;
        var latencies = new List<double>(querySet.Count);
        var annStopwatch = new Stopwatch();
        var exactStopwatch = new Stopwatch();

        for (int i = 0; i < querySet.Count; i++)
        {
            float[] query = querySet[i];

            annStopwatch.Start();
            var latencyStopwatch = Stopwatch.StartNew();
            List<long> ann = index.SearchTopK(query, topK);
            latencyStopwatch.Stop();
            annStopwatch.Stop();

            latencies.Add(latencyStopwatch.Elapsed.TotalMilliseconds);

            exactStopwatch.Start();
            List<long> exact;
            if (exactTopKBaseline != null)
            {
                exact = exactTopKBaseline[i];
            }
            else
            {
                exact = dataset
                    .Select(entry => (entry.RowId, DistanceCosine(query, entry.Vector)))
                    .OrderBy(item => item.Item2)
                    .ThenBy(item => item.RowId)
                    .Take(topK)
                    .Select(item => item.RowId)
                    .ToList();
            }

            exactStopwatch.Stop();

            int overlap = ann.Intersect(exact).Count();
            totalRecall += (double)overlap / topK;
        }

        double avgRecall = totalRecall / querySet.Count;
        double avgLatency = latencies.Average();
        double p95Latency = latencies
            .OrderBy(value => value)
            .ElementAt(Math.Min(latencies.Count - 1, (int)Math.Ceiling(latencies.Count * 0.95d) - 1));

        var snapshot = new QueryQualitySnapshot(avgRecall, avgLatency, p95Latency);
        var profile = new ProfileBreakdown(
            BuildMs: buildStopwatch.Elapsed.TotalMilliseconds,
            AnnTotalMs: annStopwatch.Elapsed.TotalMilliseconds,
            ExactTotalMs: exactStopwatch.Elapsed.TotalMilliseconds,
            BuildManagedDeltaMb: (memoryAfterBuildBytes - memoryBeforeBuildBytes) / (1024d * 1024d));

        return (snapshot, profile);
    }

    private static List<List<long>> BuildExactTopKBaseline(
        List<(long RowId, float[] Vector)> dataset,
        List<float[]> querySet,
        int topK)
    {
        var baseline = new List<List<long>>(querySet.Count);
        foreach (float[] query in querySet)
        {
            List<long> exact = dataset
                .Select(entry => (entry.RowId, DistanceCosine(query, entry.Vector)))
                .OrderBy(item => item.Item2)
                .ThenBy(item => item.RowId)
                .Take(topK)
                .Select(item => item.RowId)
                .ToList();

            baseline.Add(exact);
        }

        return baseline;
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
        var activeRows = new Dictionary<long, float[]>(vectors);
        foreach (var (rowId, vector) in dataset)
        {
            activeRows[rowId] = vector;
        }
        var snapshots = new List<QueryQualitySnapshot>
        {
            EvaluateRecallAndLatencyAtK(index, activeRows, querySet, topK)
        };

        for (int cycle = 0; cycle < churnCycles; cycle++)
        {
            List<long> removable = activeRows.Keys
                .OrderBy(_ => random.Next())
                .Take(churnBatch)
                .ToList();

            index.Delete(removable);
            for (int i = 0; i < removable.Count; i++)
            {
                activeRows.Remove(removable[i]);
            }

            for (int i = 0; i < churnBatch; i++)
            {
                float[] vector = new float[dimension];
                for (int d = 0; d < dimension; d++)
                {
                    vector[d] = (float)random.NextDouble();
                }

                long insertedId = nextId++;
                index.Insert(insertedId, vector);
                activeRows[insertedId] = vector;
            }

            snapshots.Add(EvaluateRecallAndLatencyAtK(index, activeRows, querySet, topK));
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
    private readonly record struct ProfileBreakdown(double BuildMs, double AnnTotalMs, double ExactTotalMs, double BuildManagedDeltaMb);

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

    private static float DistanceEuclidean(float[] a, float[] b)
    {
        float sum = 0f;
        for (int i = 0; i < a.Length; i++)
        {
            float diff = a[i] - b[i];
            sum += diff * diff;
        }

        return MathF.Sqrt(sum);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, recursive: true);
        }
    }
}
