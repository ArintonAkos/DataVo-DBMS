using System.Diagnostics;
using System.Globalization;
using System.Numerics;
using System.Security.Cryptography;
using System.Text;
using DataVo.Core.StorageEngine.Config;

namespace Research.Benchmark.Runners.SqlMatrix;

/// <summary>Sizes and repetition counts of one SQL-matrix run.</summary>
public sealed record SqlMatrixOptions(
    int Orders = 1_000_000,
    int Customers = 100_000,
    int Products = 1_000,
    int ScanRepetitions = 10,
    int MediumRepetitions = 100,
    int SelectiveRepetitions = 1_000);

/// <summary>One timed query on one engine.</summary>
/// <param name="Variant">Empty for the plain run, <c>columnar</c> for the re-run after DataVo created its columnar index.</param>
/// <param name="Pushed">For DataVo: whether its columnar index answered the query; <see langword="null"/> when there is no index.</param>
/// <param name="Mismatch">A description of the first wrong answer, or <see langword="null"/> when every parameter set matched the oracle.</param>
public sealed record SqlMatrixMeasurement(
    string QueryId,
    string Variant,
    int Calls,
    double P50Us,
    double P99Us,
    double MeanUs,
    double BytesPerCall,
    int Gen0,
    int Gen1,
    int Gen2,
    bool? Pushed,
    string? Mismatch,
    string? Error);

public sealed record SqlMatrixEngineResult(
    string Engine,
    string Notes,
    double LoadMs,
    double ReopenMs,
    double ReopenAgainMs,
    double SizeMb,
    double ColumnarBuildMs,
    IReadOnlyList<SqlMatrixMeasurement> Measurements,
    string? Failure);

/// <summary>
/// Runs the SQL capability matrix: every query of <see cref="SqlMatrixQueries"/> on every engine, each answer
/// checked against the oracle before it is timed.
/// </summary>
public static class SqlMatrixRunner
{
    public const string ColumnarVariant = "columnar";

    public static IReadOnlyList<ISqlMatrixEngine> CreateEngines(string filter)
    {
        string[] wanted = filter.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        bool Want(string name) => wanted.Contains("all", StringComparer.OrdinalIgnoreCase) || wanted.Contains(name, StringComparer.OrdinalIgnoreCase);
        var engines = new List<ISqlMatrixEngine>();
        if (Want("datavo")) engines.Add(new DataVoSqlMatrixEngine(StorageMode.Lsm));
        if (Want("datavo-mem")) engines.Add(new DataVoSqlMatrixEngine(StorageMode.InMemory));
        if (Want("sqlite")) engines.Add(new SqliteSqlMatrixEngine());
        if (Want("duckdb")) engines.Add(new DuckDbSqlMatrixEngine());
        return engines;
    }

    /// <summary>The oracle's fingerprint of every (query, parameter set), computed once for all engines.</summary>
    public static Dictionary<(string Query, int Set), Fingerprint> ComputeExpected(SqlMatrixData data, IReadOnlyList<SqlMatrixQuery> queries)
    {
        var expected = new Dictionary<(string, int), Fingerprint>();
        foreach (SqlMatrixQuery query in queries)
        {
            for (int set = 0; set < query.ParameterSets.Count; set++)
            {
                expected[(query.Id, set)] = Fingerprint.Of(query.Oracle(data, query.ParameterSets[set]), query.Ordered);
            }
        }

        return expected;
    }

    public static SqlMatrixEngineResult Run(
        ISqlMatrixEngine engine,
        SqlMatrixData data,
        IReadOnlyList<SqlMatrixQuery> queries,
        Dictionary<(string Query, int Set), Fingerprint> expected,
        SqlMatrixOptions options,
        string root,
        Action<string> log)
    {
        string directory = Path.Combine(root, new string(engine.Name.Where(char.IsLetterOrDigit).ToArray()));
        var measurements = new List<SqlMatrixMeasurement>();
        double loadMs = 0, reopenMs = 0, reopenAgainMs = 0, sizeMb = 0, columnarMs = 0;
        try
        {
            using (engine)
            {
                engine.Create(directory);
                Collect();
                var stopwatch = Stopwatch.StartNew();
                engine.Load(data);
                loadMs = stopwatch.Elapsed.TotalMilliseconds;
                log($"{engine.Name}: loaded {data.Orders:N0} orders in {loadMs:N0} ms");

                stopwatch.Restart();
                engine.Reopen();
                reopenMs = stopwatch.Elapsed.TotalMilliseconds;

                // The first close writes out what the load left in memory; a second close and open has nothing
                // to write, so it times opening the files on its own.
                stopwatch.Restart();
                engine.Reopen();
                reopenAgainMs = stopwatch.Elapsed.TotalMilliseconds;
                sizeMb = engine.SizeOnDiskBytes() / 1_000_000d;

                foreach (SqlMatrixQuery query in queries)
                {
                    measurements.Add(Measure(engine, query, expected, options, variant: string.Empty));
                    log($"{engine.Name}: {query.Id} {Describe(measurements[^1])}");
                }

                if (engine.SupportsColumnarIndex)
                {
                    stopwatch.Restart();
                    engine.CreateColumnarIndex();
                    columnarMs = stopwatch.Elapsed.TotalMilliseconds;
                    log($"{engine.Name}: columnar index created in {columnarMs:N0} ms");
                    foreach (SqlMatrixQuery query in queries.Where(static q => q.Columnar))
                    {
                        measurements.Add(Measure(engine, query, expected, options, ColumnarVariant));
                        log($"{engine.Name}: {query.Id}+columnar {Describe(measurements[^1])}");
                    }
                }
            }

            return new SqlMatrixEngineResult(engine.Name, engine.Notes, loadMs, reopenMs, reopenAgainMs, sizeMb, columnarMs, measurements, null);
        }
        catch (Exception exception)
        {
            log($"{engine.Name}: FAILED {exception.GetType().Name}: {exception.Message}");
            return new SqlMatrixEngineResult(engine.Name, engine.Notes, loadMs, reopenMs, reopenAgainMs, sizeMb, columnarMs, measurements, exception.Message);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { /* best effort */ }
        }
    }

    private static SqlMatrixMeasurement Measure(
        ISqlMatrixEngine engine,
        SqlMatrixQuery query,
        Dictionary<(string Query, int Set), Fingerprint> expected,
        SqlMatrixOptions options,
        string variant)
    {
        // 1. Answer check, untimed: every parameter set against the oracle. Also tells whether the
        //    columnar index answered (its scan counter moves once per pushed query).
        string? mismatch = null;
        bool? pushed = null;
        try
        {
            for (int set = 0; set < query.ParameterSets.Count; set++)
            {
                long? before = engine.ColumnarScans;
                Fingerprint actual = Fingerprint.Of(engine.Query(query, query.ParameterSets[set]), query.Ordered);
                long? after = engine.ColumnarScans;
                if (before is not null && after is not null)
                {
                    pushed = (pushed ?? true) && after > before;
                }

                Fingerprint want = expected[(query.Id, set)];
                if (mismatch is null && !actual.Equals(want))
                {
                    mismatch = $"set {set} ({FormatParameters(query.ParameterSets[set])}): expected {want.Describe()}, got {actual.Describe()}";
                }
            }
        }
        catch (Exception exception)
        {
            return new SqlMatrixMeasurement(query.Id, variant, 0, double.NaN, double.NaN, double.NaN, double.NaN, 0, 0, 0, null, null, exception.Message);
        }

        // 2. Timing: unmeasured warm-up calls, then the timed calls, cycling through the parameter sets.
        int calls = query.Cost switch
        {
            SqlMatrixCost.Selective => options.SelectiveRepetitions,
            SqlMatrixCost.Medium => options.MediumRepetitions,
            _ => options.ScanRepetitions,
        };
        int warmup = Math.Max(2, Math.Min(20, calls / 10));
        for (int i = 0; i < warmup; i++)
        {
            engine.Execute(query, query.ParameterSets[i % query.ParameterSets.Count]);
        }

        Collect();
        var latencies = new double[calls];
        int gen0 = GC.CollectionCount(0), gen1 = GC.CollectionCount(1), gen2 = GC.CollectionCount(2);
        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < calls; i++)
        {
            SqlMatrixParameter[] parameters = query.ParameterSets[i % query.ParameterSets.Count];
            long start = Stopwatch.GetTimestamp();
            engine.Execute(query, parameters);
            latencies[i] = (Stopwatch.GetTimestamp() - start) * 1_000_000d / Stopwatch.Frequency;
        }

        long allocated = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;
        int g0 = GC.CollectionCount(0) - gen0, g1 = GC.CollectionCount(1) - gen1, g2 = GC.CollectionCount(2) - gen2;
        double mean = latencies.Average();
        Array.Sort(latencies);
        double p50 = latencies[latencies.Length / 2];
        double p99 = latencies[Math.Min(latencies.Length - 1, (int)Math.Ceiling(latencies.Length * 0.99) - 1)];
        return new SqlMatrixMeasurement(query.Id, variant, calls, p50, p99, mean, (double)allocated / calls, g0, g1, g2, pushed, mismatch, null);
    }

    private static string Describe(SqlMatrixMeasurement m) =>
        m.Error is not null ? "ERROR " + m.Error
        : $"p50 {Latency(m.P50Us)} p99 {Latency(m.P99Us)} alloc {Bytes(m.BytesPerCall)}/call{(m.Pushed is { } p ? (p ? " [pushed]" : " [row scan]") : "")}{(m.Mismatch is null ? "" : " MISMATCH " + m.Mismatch)}";

    public static string ToMarkdown(
        IReadOnlyList<SqlMatrixEngineResult> results,
        IReadOnlyList<SqlMatrixQuery> queries,
        SqlMatrixOptions options)
    {
        var b = new StringBuilder();
        b.AppendLine(CultureInfo.InvariantCulture, $"## SQL capability matrix: {options.Orders:N0} orders, {options.Customers:N0} customers, {options.Products:N0} products");
        b.AppendLine();
        b.AppendLine(CultureInfo.InvariantCulture, $"Machine: {Environment.ProcessorCount} logical cores, {System.Runtime.InteropServices.RuntimeInformation.OSDescription}, {System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture}, .NET {Environment.Version}. Run at {DateTimeOffset.Now:yyyy-MM-dd HH:mm zzz}.");
        b.AppendLine();
        b.AppendLine("Cells are the median latency of one call (p99 in brackets). `✗` marks an answer that differs from the oracle; such a cell is not a result. The last column is SQLite's median divided by DataVo LSM's (above 1: DataVo LSM is faster).");
        b.AppendLine();

        Header(b, "Setup", results, extra: null);
        Row(b, "Load (ms)", results, r => r.LoadMs.ToString("N0", CultureInfo.InvariantCulture), null);
        Row(b, "Close and reopen after the load (ms)", results, r => r.ReopenMs.ToString("N1", CultureInfo.InvariantCulture), null);
        Row(b, "Close and reopen again, nothing to write (ms)", results, r => r.ReopenAgainMs.ToString("N1", CultureInfo.InvariantCulture), null);
        Row(b, "Size on disk (MB)", results, r => r.SizeMb > 0 ? r.SizeMb.ToString("N1", CultureInfo.InvariantCulture) : "in memory", null);
        Row(b, "Create columnar index (ms)", results, r => r.ColumnarBuildMs > 0 ? r.ColumnarBuildMs.ToString("N0", CultureInfo.InvariantCulture) : "-", null);
        b.AppendLine();

        foreach (IGrouping<string, SqlMatrixQuery> section in queries.GroupBy(static q => q.Section))
        {
            b.AppendLine(CultureInfo.InvariantCulture, $"### {section.Key}");
            b.AppendLine();
            Header(b, "Query", results, extra: "SQLite / DataVo LSM");
            foreach (SqlMatrixQuery query in section)
            {
                LatencyRow(b, query, string.Empty, results);
                if (query.Columnar)
                {
                    LatencyRow(b, query, ColumnarVariant, results);
                }
            }

            b.AppendLine();
        }

        b.AppendLine("### Managed allocation per call and GC collections during the timed calls");
        b.AppendLine();
        b.AppendLine("Bytes allocated on the calling thread per call (`GC.GetAllocatedBytesForCurrentThread`), with gen0/gen1/gen2 collections over all timed calls. SQLite's and DuckDB's native memory is not visible to this counter; their figures are only what the ADO.NET layer allocates.");
        b.AppendLine();
        Header(b, "Query", results, extra: null);
        foreach (SqlMatrixQuery query in queries)
        {
            foreach (string variant in query.Columnar ? new[] { string.Empty, ColumnarVariant } : [string.Empty])
            {
                b.Append("| ").Append(Label(query, variant)).Append(" |");
                foreach (SqlMatrixEngineResult result in results)
                {
                    SqlMatrixMeasurement? m = Find(result, query.Id, variant);
                    b.Append(' ').Append(m is null || m.Error is not null ? "-" : $"{Bytes(m.BytesPerCall)} ({m.Gen0}/{m.Gen1}/{m.Gen2})").Append(" |");
                }

                b.AppendLine();
            }
        }

        b.AppendLine();
        b.AppendLine("### Answer check");
        b.AppendLine();
        bool any = false;
        foreach (SqlMatrixEngineResult result in results)
        {
            if (result.Failure is not null)
            {
                any = true;
                b.AppendLine(CultureInfo.InvariantCulture, $"- **{result.Engine}** FAILED: {result.Failure}");
            }

            foreach (SqlMatrixMeasurement m in result.Measurements)
            {
                if (m.Mismatch is not null || m.Error is not null)
                {
                    any = true;
                    b.AppendLine(CultureInfo.InvariantCulture, $"- **{result.Engine}**, {m.QueryId}{(m.Variant.Length > 0 ? " + " + m.Variant : "")}: {(m.Error is not null ? "ERROR " + m.Error : "MISMATCH " + m.Mismatch)}");
                }
            }
        }

        if (!any)
        {
            b.AppendLine("- Every engine returned the oracle's answer for every query and parameter set.");
        }
        else
        {
            b.AppendLine("- Every other (engine, query) pair returned the oracle's answer for every parameter set.");
        }

        b.AppendLine();
        b.AppendLine("### Columnar pushdown (DataVo)");
        b.AppendLine();
        foreach (SqlMatrixEngineResult result in results.Where(static r => r.Measurements.Any(static m => m.Variant == ColumnarVariant)))
        {
            foreach (SqlMatrixMeasurement m in result.Measurements.Where(static m => m.Variant == ColumnarVariant))
            {
                b.AppendLine(CultureInfo.InvariantCulture, $"- {result.Engine}, {m.QueryId}: {(m.Pushed == true ? "answered by the columnar index" : "row scan (index not used)")}");
            }
        }

        b.AppendLine();
        b.AppendLine("### Engines");
        b.AppendLine();
        foreach (SqlMatrixEngineResult result in results)
        {
            b.AppendLine(CultureInfo.InvariantCulture, $"- **{result.Engine}**: {result.Notes}");
        }

        b.AppendLine();
        b.AppendLine("### Queries");
        b.AppendLine();
        b.AppendLine(CultureInfo.InvariantCulture, $"Timed calls: {options.ScanRepetitions} for scans, {options.MediumRepetitions} for 10,000-row ranges, {options.SelectiveRepetitions} for selective queries, after unmeasured warm-up calls; parameter sets are cycled.");
        b.AppendLine();
        foreach (SqlMatrixQuery query in queries)
        {
            b.AppendLine(CultureInfo.InvariantCulture, $"- **{query.Id}** ({query.Title}): `{query.Sql}` with {string.Join("; ", query.ParameterSets.Select(FormatParameters).Where(static s => s.Length > 0).DefaultIfEmpty("no parameters"))}");
        }

        return b.ToString();
    }

    private static void LatencyRow(StringBuilder b, SqlMatrixQuery query, string variant, IReadOnlyList<SqlMatrixEngineResult> results)
    {
        b.Append("| ").Append(Label(query, variant)).Append(" |");
        foreach (SqlMatrixEngineResult result in results)
        {
            SqlMatrixMeasurement? m = Find(result, query.Id, variant);
            string cell = m is null ? (variant.Length > 0 ? "" : "-")
                : m.Error is not null ? "error"
                : $"{Latency(m.P50Us)} ({Latency(m.P99Us)}){(m.Mismatch is null ? "" : " ✗")}{(m.Pushed is { } p && variant.Length > 0 ? (p ? " pushed" : " row scan") : "")}";
            b.Append(' ').Append(cell).Append(" |");
        }

        SqlMatrixMeasurement? sqlite = results.FirstOrDefault(static r => r.Engine == "SQLite") is { } s ? Find(s, query.Id, string.Empty) : null;
        SqlMatrixMeasurement? lsm = results.FirstOrDefault(static r => r.Engine.StartsWith("DataVo (LSM)", StringComparison.Ordinal)) is { } d ? Find(d, query.Id, variant) : null;
        string ratio = sqlite is { Error: null, Mismatch: null } && lsm is { Error: null, Mismatch: null } && lsm.P50Us > 0
            ? (sqlite.P50Us / lsm.P50Us).ToString("0.00", CultureInfo.InvariantCulture) + "x"
            : "n/a";
        b.Append(' ').Append(ratio).AppendLine(" |");
    }

    private static string Label(SqlMatrixQuery query, string variant) =>
        variant.Length == 0 ? $"{query.Id} {query.Title}" : $"{query.Id} + columnar index";

    private static SqlMatrixMeasurement? Find(SqlMatrixEngineResult result, string queryId, string variant) =>
        result.Measurements.FirstOrDefault(m => m.QueryId == queryId && m.Variant == variant);

    private static void Header(StringBuilder b, string first, IReadOnlyList<SqlMatrixEngineResult> results, string? extra)
    {
        b.Append("| ").Append(first).Append(" |");
        foreach (SqlMatrixEngineResult result in results) b.Append(' ').Append(result.Engine).Append(" |");
        if (extra is not null) b.Append(' ').Append(extra).Append(" |");
        b.AppendLine();
        b.Append("|---|");
        foreach (SqlMatrixEngineResult _ in results) b.Append("---:|");
        if (extra is not null) b.Append("---:|");
        b.AppendLine();
    }

    private static void Row(StringBuilder b, string label, IReadOnlyList<SqlMatrixEngineResult> results, Func<SqlMatrixEngineResult, string> cell, string? extra)
    {
        b.Append("| ").Append(label).Append(" |");
        foreach (SqlMatrixEngineResult result in results) b.Append(' ').Append(cell(result)).Append(" |");
        if (extra is not null) b.Append(' ').Append(extra).Append(" |");
        b.AppendLine();
    }

    private static string FormatParameters(SqlMatrixParameter[] parameters) =>
        string.Join(", ", parameters.Select(static p => $"@{p.Name}={Convert.ToString(p.Value, CultureInfo.InvariantCulture)}"));

    public static string Latency(double us) => double.IsNaN(us) ? "-" : us switch
    {
        >= 1_000_000 => (us / 1_000_000).ToString("N2", CultureInfo.InvariantCulture) + " s",
        >= 1_000 => (us / 1_000).ToString("N2", CultureInfo.InvariantCulture) + " ms",
        _ => us.ToString("N1", CultureInfo.InvariantCulture) + " µs",
    };

    public static string Bytes(double bytes) => double.IsNaN(bytes) ? "-" : bytes switch
    {
        >= 1_048_576 => (bytes / 1_048_576).ToString("N1", CultureInfo.InvariantCulture) + " MB",
        >= 1_024 => (bytes / 1_024).ToString("N1", CultureInfo.InvariantCulture) + " KB",
        _ => bytes.ToString("N0", CultureInfo.InvariantCulture) + " B",
    };

    public static long DirectorySize(string directory) =>
        Directory.Exists(directory)
            ? Directory.EnumerateFiles(directory, "*", SearchOption.AllDirectories).Sum(static f => new FileInfo(f).Length)
            : 0;

    private static void Collect()
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
    }

    /// <summary>
    /// An engine-independent digest of a result: each cell normalised (whole numbers as integers whatever
    /// their CLR type, other numbers in round-trip form, NULL as <c>NULL</c>), rows sorted unless the query
    /// fixes their order, then hashed. The first rows are kept for the report.
    /// </summary>
    public readonly record struct Fingerprint(int Rows, string Hash, string Preview)
    {
        public static Fingerprint Of(List<object?[]> rows, bool ordered)
        {
            var lines = new List<string>(rows.Count);
            foreach (object?[] row in rows)
            {
                lines.Add(string.Join("|", row.Select(Cell)));
            }

            if (!ordered)
            {
                lines.Sort(StringComparer.Ordinal);
            }

            string joined = string.Join("\n", lines);
            string hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(joined)))[..16];
            return new Fingerprint(lines.Count, hash, string.Join(" / ", lines.Take(3)));
        }

        public bool Equals(Fingerprint other) => Rows == other.Rows && Hash == other.Hash;

        public override int GetHashCode() => HashCode.Combine(Rows, Hash);

        public string Describe() => $"{Rows} row(s) [{(Preview.Length > 120 ? Preview[..120] + "..." : Preview)}]";

        private static string Cell(object? value)
        {
            double number;
            switch (value)
            {
                case null or DBNull:
                    return "NULL";
                case string s:
                    return s;
                case bool flag:
                    return flag ? "1" : "0";
                case BigInteger big:
                    number = (double)big;
                    break;
                case IConvertible convertible when value is not char:
                    number = convertible.ToDouble(CultureInfo.InvariantCulture);
                    break;
                default:
                    return Convert.ToString(value, CultureInfo.InvariantCulture) ?? "";
            }

            return number == Math.Floor(number) && Math.Abs(number) < 1e15
                ? ((long)number).ToString(CultureInfo.InvariantCulture)
                : number.ToString("R", CultureInfo.InvariantCulture);
        }
    }
}
