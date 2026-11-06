using System.Diagnostics;
using System.Globalization;
using System.Text;

namespace Research.Benchmark.Runners.SqlWorkload;

public sealed record SqlWorkloadOptions(
    int Orders = 1_000_000,
    int PointReads = 20_000,
    int SecondaryReads = 2_000,
    int RangeScans = 2_000,
    int ScanQueries = 5,
    int Updates = 10_000,
    int Deletes = 5_000);

/// <summary>One measured line of the report: a total wall time and, for repeated operations, latencies.</summary>
public sealed record SqlWorkloadMeasurement(string Operation, double TotalMs, int Count, double P50Us, double P99Us, long Checksum);

public sealed record SqlWorkloadEngineResult(string Engine, string Notes, IReadOnlyList<SqlWorkloadMeasurement> Measurements, string? Failure);

public static class SqlWorkloadRunner
{
    public static IReadOnlyList<ISqlWorkloadEngine> CreateEngines(string filter)
    {
        bool all = filter.Equals("all", StringComparison.OrdinalIgnoreCase);
        var engines = new List<ISqlWorkloadEngine>();
        if (all || filter.Equals("datavo", StringComparison.OrdinalIgnoreCase)) engines.Add(new DataVoSqlWorkloadEngine());
        if (all || filter.Equals("sqlite", StringComparison.OrdinalIgnoreCase)) engines.Add(new SqliteSqlWorkloadEngine());
        if (all || filter.Equals("duckdb", StringComparison.OrdinalIgnoreCase)) engines.Add(new DuckDbSqlWorkloadEngine());
        if (all || filter.Equals("litedb", StringComparison.OrdinalIgnoreCase)) engines.Add(new LiteDbSqlWorkloadEngine());
        return engines;
    }

    public static SqlWorkloadEngineResult Run(ISqlWorkloadEngine engine, SqlWorkloadOptions options, string root, Action<string> log)
    {
        var data = new SqlWorkloadData(options.Orders);
        var results = new List<SqlWorkloadMeasurement>();
        string directory = Path.Combine(root, new string(engine.Name.Where(char.IsLetterOrDigit).ToArray()));
        try
        {
            using (engine)
            {
                engine.Create(directory);
                results.Add(Once("Load orders + customers", () => { engine.LoadOrders(data); return data.Orders; }));
                log($"{engine.Name}: loaded {data.Orders:N0} orders");
                results.Add(Once("Load GUID-keyed rows", () => { engine.LoadGuids(data); return data.Guids.Length; }));
                log($"{engine.Name}: loaded {data.Guids.Length:N0} GUID rows");

                var random = new Random(7);
                results.Add(Repeat("Point read by INT key", options.PointReads, _ => engine.PointSelect(random.Next(1, data.Orders + 1))));
                random = new Random(8);
                results.Add(Repeat("Point read by GUID key", options.PointReads, _ => engine.GuidPointSelect(data.Guids[random.Next(data.Guids.Length)])));
                random = new Random(9);
                results.Add(Repeat("Secondary-index lookup (~100 rows)", options.SecondaryReads, _ => engine.SecondaryLookup(random.Next(1, data.Customers + 1))));
                random = new Random(10);
                results.Add(Repeat("Key range scan (100 rows)", options.RangeScans, _ => engine.RangeScan(random.Next(1, data.Orders - 100), 100)));
                log($"{engine.Name}: point and range reads done");
                results.Add(Repeat("COUNT with filter (full scan)", options.ScanQueries, _ => engine.FilteredCount(), warmupCalls: 2));
                results.Add(Repeat("GROUP BY (full scan)", options.ScanQueries, _ => engine.GroupByStatus(), warmupCalls: 2));
                results.Add(Repeat("JOIN + GROUP BY", options.ScanQueries, _ => engine.JoinByRegion(), warmupCalls: 2));
                results.Add(Repeat("ORDER BY ... LIMIT 10", options.ScanQueries, _ => engine.TopTen(), warmupCalls: 2));
                log($"{engine.Name}: analytic queries done");

                random = new Random(11);
                results.Add(Repeat("UPDATE by key (autocommit)", options.Updates, i =>
                {
                    engine.UpdateAmount(random.Next(1, data.Orders + 1), i * 0.5d);
                    return 1;
                }));
                // Delete distinct keys from the top of the range, so no delete misses.
                results.Add(Repeat("DELETE by key (autocommit)", options.Deletes, i =>
                {
                    engine.Delete(data.Orders - i);
                    return 1;
                }));
                log($"{engine.Name}: writes done");

                results.Add(Once("Close (flush to disk)", () => { engine.Close(); return 0; }));
                long size = engine.SizeOnDiskBytes();
                results.Add(new SqlWorkloadMeasurement("Size on disk (MB)", size / 1_000_000d, 0, 0, 0, 0));
                results.Add(Once("Reopen + first point read", () =>
                {
                    engine.Reopen();
                    return engine.PointSelect(1);
                }));
                results.Add(Repeat("Point read after reopen", Math.Min(options.PointReads, 5_000), _ => engine.PointSelect(random.Next(1, data.Orders - options.Deletes))));
                engine.Close();
            }

            return new SqlWorkloadEngineResult(engine.Name, engine.Notes, results, null);
        }
        catch (Exception exception)
        {
            log($"{engine.Name}: FAILED {exception.GetType().Name}: {exception.Message}");
            return new SqlWorkloadEngineResult(engine.Name, engine.Notes, results, exception.Message);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { /* best effort */ }
        }
    }

    public static long DirectorySize(string directory) =>
        Directory.Exists(directory)
            ? Directory.EnumerateFiles(directory, "*", SearchOption.AllDirectories).Sum(static f => new FileInfo(f).Length)
            : 0;

    public static string ToMarkdown(IReadOnlyList<SqlWorkloadEngineResult> results, SqlWorkloadOptions options)
    {
        var builder = new StringBuilder();
        builder.AppendLine(CultureInfo.InvariantCulture, $"## SQL workload, {options.Orders:N0} orders ({Math.Max(100, options.Orders / 100):N0} customers, {options.Orders:N0} GUID rows)");
        builder.AppendLine();
        builder.Append("| Operation |");
        foreach (SqlWorkloadEngineResult result in results) builder.Append(' ').Append(result.Engine).Append(" |");
        builder.AppendLine();
        builder.Append("|---|");
        foreach (SqlWorkloadEngineResult _ in results) builder.Append("---|");
        builder.AppendLine();

        IEnumerable<string> operations = results.SelectMany(static r => r.Measurements.Select(static m => m.Operation)).Distinct();
        foreach (string operation in operations)
        {
            builder.Append("| ").Append(operation).Append(" |");
            foreach (SqlWorkloadEngineResult result in results)
            {
                SqlWorkloadMeasurement? m = result.Measurements.FirstOrDefault(x => x.Operation == operation);
                builder.Append(' ').Append(m is null ? "failed" : Format(m)).Append(" |");
            }

            builder.AppendLine();
        }

        builder.AppendLine();
        builder.AppendLine("Repeated operations show the median and p99 latency of one call; single steps show total time.");
        builder.AppendLine();
        foreach (SqlWorkloadEngineResult result in results)
        {
            builder.Append("- **").Append(result.Engine).Append("**: ").Append(result.Notes);
            if (result.Failure is not null) builder.Append(" FAILED: ").Append(result.Failure);
            builder.AppendLine();
        }

        builder.AppendLine();
        builder.AppendLine("Result check (the same query must give the same answer everywhere):");
        builder.AppendLine();
        foreach (string operation in operations)
        {
            var sums = results
                .Select(r => (r.Engine, Sum: r.Measurements.FirstOrDefault(x => x.Operation == operation)?.Checksum))
                .Where(static x => x.Sum is not null)
                .ToList();
            if (sums.Select(static x => x.Sum).Distinct().Count() > 1)
            {
                builder.AppendLine(CultureInfo.InvariantCulture, $"- MISMATCH in '{operation}': {string.Join(", ", sums.Select(static x => $"{x.Engine}={x.Sum}"))}");
            }
        }

        builder.AppendLine("- every other operation returned identical results on all engines.");
        return builder.ToString();
    }

    private static string Format(SqlWorkloadMeasurement m)
    {
        if (m.Operation.StartsWith("Size", StringComparison.Ordinal))
        {
            return m.TotalMs.ToString("N1", CultureInfo.InvariantCulture) + " MB";
        }

        if (m.Count <= 1)
        {
            return m.TotalMs >= 1000
                ? (m.TotalMs / 1000).ToString("N2", CultureInfo.InvariantCulture) + " s"
                : m.TotalMs.ToString("N1", CultureInfo.InvariantCulture) + " ms";
        }

        return $"{Latency(m.P50Us)} (p99 {Latency(m.P99Us)})";
    }

    private static string Latency(double us) => us switch
    {
        >= 1_000_000 => (us / 1_000_000).ToString("N2", CultureInfo.InvariantCulture) + " s",
        >= 1_000 => (us / 1_000).ToString("N1", CultureInfo.InvariantCulture) + " ms",
        _ => us.ToString("N1", CultureInfo.InvariantCulture) + " µs",
    };

    private static SqlWorkloadMeasurement Once(string operation, Func<long> action)
    {
        Collect();
        var stopwatch = Stopwatch.StartNew();
        long checksum = action();
        return new SqlWorkloadMeasurement(operation, stopwatch.Elapsed.TotalMilliseconds, 1, 0, 0, checksum);
    }

    private static SqlWorkloadMeasurement Repeat(string operation, int count, Func<int, long> action, int warmupCalls = 0)
    {
        // Unmeasured calls first, so no engine is timed while it is still compiling its code path.
        for (int i = 0; i < warmupCalls; i++)
        {
            action(i);
        }

        Collect();
        // Warm up with a few calls that are not measured (JIT, statement preparation, caches).
        int warmup = Math.Min(3, count);
        var latencies = new double[count];
        long checksum = 0;
        var total = Stopwatch.StartNew();
        for (int i = 0; i < count; i++)
        {
            long start = Stopwatch.GetTimestamp();
            checksum += action(i);
            latencies[i] = (Stopwatch.GetTimestamp() - start) * 1_000_000d / Stopwatch.Frequency;
        }

        total.Stop();
        double[] measured = count > warmup * 4 ? latencies[warmup..] : latencies;
        Array.Sort(measured);
        double p50 = measured[measured.Length / 2];
        double p99 = measured[Math.Min(measured.Length - 1, (int)Math.Ceiling(measured.Length * 0.99) - 1)];
        return new SqlWorkloadMeasurement(operation, total.Elapsed.TotalMilliseconds, count, p50, p99, checksum);
    }

    private static void Collect()
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
    }
}
