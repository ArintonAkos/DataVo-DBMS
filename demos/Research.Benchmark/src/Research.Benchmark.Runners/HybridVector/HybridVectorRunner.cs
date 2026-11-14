using System.Diagnostics;
using System.Globalization;
using System.Text;
using Research.Benchmark.Runners.SqlMatrix;

namespace Research.Benchmark.Runners.HybridVector;

public sealed record HybridVectorOptions(int Documents = 50_000, int Dimensions = 384, int Queries = 200, int Warmup = 10);

/// <summary>Latency and answer quality of one query shape on one engine.</summary>
/// <param name="ShortResults">Queries that returned fewer rows than min(10, rows matching the filter).</param>
/// <param name="InvalidRows">Returned rows that do not satisfy the filter (always a bug).</param>
public sealed record HybridShapeResult(
    HybridShape Shape,
    double P50Us,
    double P99Us,
    double MeanRecall,
    double MinRecall,
    int ShortResults,
    int InvalidRows,
    double BytesPerQuery,
    string? Routing,
    string? Error);

public sealed record HybridEngineResult(string Engine, string Notes, double LoadMs, IReadOnlyList<HybridShapeResult> Shapes, IReadOnlyDictionary<HybridShape, string> Sql, string? Failure);

/// <summary>
/// Runs every <see cref="HybridShape"/> on every engine and scores each answer against the exact filtered
/// top-10 (recall@10): a returned row counts when it is in the true top-10, or ties its 10th distance.
/// </summary>
public static class HybridVectorRunner
{
    private const float TieTolerance = 1e-5f;

    public static IReadOnlyList<IHybridVectorEngine> CreateEngines(string filter)
    {
        string[] wanted = filter.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        bool Want(string name) => wanted.Contains("all", StringComparer.OrdinalIgnoreCase) || wanted.Contains(name, StringComparer.OrdinalIgnoreCase);
        var engines = new List<IHybridVectorEngine>();
        if (Want("datavo-flat")) engines.Add(new DataVoHybridVectorEngine("FLAT"));
        if (Want("datavo-hnsw")) engines.Add(new DataVoHybridVectorEngine("HNSW"));
        if (Want("sqlite")) engines.Add(new SqliteVecHybridVectorEngine());
        return engines;
    }

    /// <summary>Every query's distances to every document, shared by all engines' recall checks.</summary>
    public static float[][] ComputeDistances(HybridVectorData data) =>
        [.. data.QueryVectors.AsParallel().AsOrdered().Select(data.Distances)];

    public static HybridEngineResult Run(IHybridVectorEngine engine, HybridVectorData data, float[][] distances, HybridVectorOptions options, string root, Action<string> log)
    {
        string directory = Path.Combine(root, new string(engine.Name.Where(char.IsLetterOrDigit).ToArray()));
        var shapes = new List<HybridShapeResult>();
        var sql = new Dictionary<HybridShape, string>();
        double loadMs = 0;
        try
        {
            using (engine)
            {
                engine.Initialize(directory, data.Dimensions);
                GC.Collect();
                var stopwatch = Stopwatch.StartNew();
                engine.Load(data);
                loadMs = stopwatch.Elapsed.TotalMilliseconds;
                log($"{engine.Name}: loaded {data.Documents:N0} x {data.Dimensions}-dim documents (index build included) in {loadMs:N0} ms");

                foreach (HybridShape shape in Enum.GetValues<HybridShape>())
                {
                    sql[shape] = engine.SqlFor(shape);
                    shapes.Add(MeasureShape(engine, data, distances, options, shape));
                    HybridShapeResult r = shapes[^1];
                    log($"{engine.Name}: {shape} {(r.Error is not null ? "ERROR " + r.Error : $"p50 {SqlMatrixRunner.Latency(r.P50Us)} p99 {SqlMatrixRunner.Latency(r.P99Us)} recall {r.MeanRecall:P1} (min {r.MinRecall:P0}) short {r.ShortResults} invalid {r.InvalidRows}")}{(r.Routing is null ? "" : " | " + r.Routing)}");
                }
            }

            return new HybridEngineResult(engine.Name, engine.Notes, loadMs, shapes, sql, null);
        }
        catch (Exception exception)
        {
            log($"{engine.Name}: FAILED {exception.GetType().Name}: {exception.Message}");
            return new HybridEngineResult(engine.Name, engine.Notes, loadMs, shapes, sql, exception.Message);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { /* best effort */ }
        }
    }

    private static HybridShapeResult MeasureShape(IHybridVectorEngine engine, HybridVectorData data, float[][] distances, HybridVectorOptions options, HybridShape shape)
    {
        int queries = data.QueryVectors.Length;
        try
        {
            for (int i = 0; i < options.Warmup; i++)
            {
                engine.Search(shape, data.QueryVectors[i % queries], i % queries);
            }

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            var answers = new IReadOnlyList<long>[queries];
            var latencies = new double[queries];
            long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
            for (int q = 0; q < queries; q++)
            {
                long start = Stopwatch.GetTimestamp();
                answers[q] = engine.Search(shape, data.QueryVectors[q], q);
                latencies[q] = (Stopwatch.GetTimestamp() - start) * 1_000_000d / Stopwatch.Frequency;
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;

            // Quality, after timing.
            double recallSum = 0, minRecall = 1;
            int shortResults = 0, invalidRows = 0;
            for (int q = 0; q < queries; q++)
            {
                (int[] truth, float kth, int matching) = data.Truth(shape, q, distances[q]);
                int wanted = Math.Min(HybridVectorData.TopK, matching);
                var truthSet = truth.ToHashSet();
                int hits = 0;
                foreach (long id in answers[q].Take(HybridVectorData.TopK))
                {
                    if (id < 1 || id > data.Documents || !data.Matches(shape, (int)id, q))
                    {
                        invalidRows++;
                        continue;
                    }

                    if (truthSet.Contains((int)id) || distances[q][id] <= kth + TieTolerance)
                    {
                        hits++;
                    }
                }

                double recall = wanted == 0 ? 1 : Math.Min(1d, (double)hits / wanted);
                recallSum += recall;
                minRecall = Math.Min(minRecall, recall);
                if (answers[q].Count < wanted)
                {
                    shortResults++;
                }
            }

            Array.Sort(latencies);
            double p50 = latencies[latencies.Length / 2];
            double p99 = latencies[Math.Min(latencies.Length - 1, (int)Math.Ceiling(latencies.Length * 0.99) - 1)];
            return new HybridShapeResult(shape, p50, p99, recallSum / queries, minRecall, shortResults, invalidRows, (double)allocated / queries, DescribeRouting(engine, data, shape), null);
        }
        catch (Exception exception)
        {
            return new HybridShapeResult(shape, double.NaN, double.NaN, double.NaN, double.NaN, 0, 0, double.NaN, null, exception.Message);
        }
    }

    /// <summary>Runs every query once more with diagnostics on (untimed) and summarises the planner's routes.</summary>
    private static string? DescribeRouting(IHybridVectorEngine engine, HybridVectorData data, HybridShape shape)
    {
        var routes = new List<HybridRouting>();
        for (int q = 0; q < data.QueryVectors.Length; q++)
        {
            if (engine.Explain(shape, data.QueryVectors[q], q) is not { } routing)
            {
                return null;
            }

            routes.Add(routing);
        }

        int[] topK = [.. routes.Select(static r => r.VectorTopK).Order()];
        var buckets = routes.SelectMany(static r => r.Buckets).GroupBy(static b => b).OrderBy(static g => g.Key, StringComparer.Ordinal)
            .Select(g => $"{g.Key} {g.Count()}/{routes.Count}");
        return string.Create(CultureInfo.InvariantCulture,
            $"vector index {routes.Count(static r => r.VectorIndexUsed)}/{routes.Count}; candidates (top-k) median {topK[topK.Length / 2]} (max {topK[^1]}); "
            + $"expansion passes mean {routes.Average(static r => r.ExpansionPasses):0.00}; full table scan {routes.Count(static r => r.FullTableScan)}/{routes.Count}; "
            + $"routes: {string.Join(", ", buckets)}");
    }

    public static string ToMarkdown(IReadOnlyList<HybridEngineResult> results, HybridVectorData data, HybridVectorOptions options)
    {
        var b = new StringBuilder();
        b.AppendLine(CultureInfo.InvariantCulture, $"## Hybrid vector + SQL: {data.Documents:N0} documents x {data.Dimensions} dimensions (clustered), {data.Authors:N0} authors, {options.Queries} held-out queries per shape");
        b.AppendLine();
        b.AppendLine(CultureInfo.InvariantCulture, $"Machine: {Environment.ProcessorCount} logical cores, {System.Runtime.InteropServices.RuntimeInformation.OSDescription}, {System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture}, .NET {Environment.Version}. Run at {DateTimeOffset.Now:yyyy-MM-dd HH:mm zzz}.");
        b.AppendLine();
        b.AppendLine("Recall@10 is measured against the exact top-10 among the rows that satisfy the query's WHERE clause (cosine distance). *Short* counts queries that returned fewer than min(10, matching rows) rows; *invalid* counts returned rows that fail the filter.");
        b.AppendLine();
        b.AppendLine("| Engine | Load + index build |");
        b.AppendLine("|---|---:|");
        foreach (HybridEngineResult r in results)
        {
            b.AppendLine(CultureInfo.InvariantCulture, $"| {r.Engine} | {r.LoadMs:N0} ms{(r.Failure is null ? "" : " (FAILED: " + r.Failure + ")")} |");
        }

        b.AppendLine();
        foreach (HybridShape shape in Enum.GetValues<HybridShape>())
        {
            b.AppendLine(CultureInfo.InvariantCulture, $"### {ShapeTitle(shape)}");
            b.AppendLine();
            b.AppendLine("| Engine | p50 | p99 | Recall@10 mean | min | Short | Invalid | Alloc/query |");
            b.AppendLine("|---|---:|---:|---:|---:|---:|---:|---:|");
            foreach (HybridEngineResult r in results)
            {
                HybridShapeResult? s = r.Shapes.FirstOrDefault(x => x.Shape == shape);
                if (s is null)
                {
                    continue;
                }

                b.AppendLine(s.Error is not null
                    ? $"| {r.Engine} | error: {s.Error} | | | | | | |"
                    : string.Create(CultureInfo.InvariantCulture, $"| {r.Engine} | {SqlMatrixRunner.Latency(s.P50Us)} | {SqlMatrixRunner.Latency(s.P99Us)} | {s.MeanRecall:P1} | {s.MinRecall:P0} | {s.ShortResults} | {s.InvalidRows} | {SqlMatrixRunner.Bytes(s.BytesPerQuery)} |"));
            }

            b.AppendLine();
            foreach (HybridEngineResult r in results)
            {
                if (r.Shapes.FirstOrDefault(x => x.Shape == shape)?.Routing is { } routing)
                {
                    b.AppendLine(CultureInfo.InvariantCulture, $"- {r.Engine} planner: {routing}");
                }
            }

            foreach (HybridEngineResult r in results)
            {
                if (r.Sql.TryGetValue(shape, out string? text))
                {
                    b.AppendLine(CultureInfo.InvariantCulture, $"- {r.Engine} SQL: `{text}`");
                }
            }

            b.AppendLine();
        }

        b.AppendLine("### Engines");
        b.AppendLine();
        foreach (HybridEngineResult r in results)
        {
            b.AppendLine(CultureInfo.InvariantCulture, $"- **{r.Engine}**: {r.Notes}");
        }

        return b.ToString();
    }

    private static string ShapeTitle(HybridShape shape) => shape switch
    {
        HybridShape.V0Unfiltered => "V0: top-10, no filter",
        HybridShape.V1Selective => "V1: top-10 WHERE Category = c AND Price < 50 (about 5% of rows)",
        HybridShape.V2Broad => "V2: top-10 WHERE Price < 50 (about 50% of rows)",
        HybridShape.V3JoinDocFilter => "V3: V1 + JOIN Authors (filter on the documents)",
        HybridShape.V4JoinAuthorFilter => "V4: JOIN Authors WHERE a.Country = k (about 10% of rows, filter on the joined table)",
        _ => shape.ToString(),
    };
}
