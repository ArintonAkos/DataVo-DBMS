using System.Diagnostics;
using Research.Benchmark.Abstractions;
using Research.Benchmark.Host;
using Research.Benchmark.Runners;
using Research.Benchmark.Runners.ComplexVip;
using Research.Benchmark.Runners.DataVo;
using Research.Benchmark.Runners.DuckDb;
using Research.Benchmark.Runners.FlatCrud;
using Research.Benchmark.Runners.Sqlite;

string benchmarkScenario = ReadStringArg(args, "--scenario", "complex-vip");
int baselineOrders = ReadIntArg(args, "--baseline", 10_000);
int iterations = ReadIntArg(args, "--iterations", 50_000);
int progressEvery = ReadIntArg(args, "--progress-every", 1_000);
string engineFilter = ReadStringArg(args, "--engine", "all").ToLowerInvariant();
string outputFormat = ReadStringArg(args, "--format", "markdown").ToLowerInvariant();

List<BenchmarkMetrics> results = [];

if (benchmarkScenario.Equals("simple-exposure", StringComparison.OrdinalIgnoreCase))
{
    var scenario = new BettingRiskScenario(
        MarketCount: 100,
        RunnersPerMarket: 24,
        AccountCount: 10_000,
        InitialOrderCount: baselineOrders,
        SubscriberCount: 2_500);

    if (ShouldRun(engineFilter, "datavo"))
        results.Add(await RunSimpleAsync(new DataVoEngine(), scenario, iterations, progressEvery));
    if (ShouldRun(engineFilter, "duckdb"))
        results.Add(await RunSimpleAsync(new DuckDbEngine(), scenario, iterations, progressEvery));
    if (ShouldRun(engineFilter, "sqlite"))
        results.Add(await RunSimpleAsync(new SqliteEngine(), scenario, iterations, progressEvery));
}
else if (benchmarkScenario.Equals("complex-vip", StringComparison.OrdinalIgnoreCase))
{
    var scenario = new ComplexVipExposureScenario(
        InitialOrderCount: baselineOrders,
        AccountCount: 1_000,
        MarketCount: 50,
        VipRatio: 0.20d);

    if (ShouldRun(engineFilter, "datavo"))
        results.Add(await RunComplexAsync(new DataVoComplexVipExposureEngine(), scenario, iterations, progressEvery));
    if (ShouldRun(engineFilter, "duckdb"))
        results.Add(await RunComplexAsync(new DuckDbComplexVipExposureEngine(), scenario, iterations, progressEvery));
    if (ShouldRun(engineFilter, "sqlite"))
        results.Add(await RunComplexAsync(new SqliteComplexVipExposureEngine(), scenario, iterations, progressEvery));
}
else if (benchmarkScenario.Equals("flat-crud", StringComparison.OrdinalIgnoreCase))
{
    int records = ReadIntArg(args, "--records", 50_000);

    if (ShouldRun(engineFilter, "datavo"))
        results.Add(RunFlatCrud(new DataVoFlatCrudEngine(), records, progressEvery));
    if (ShouldRun(engineFilter, "litedb"))
        results.Add(RunFlatCrud(new LiteDbFlatCrudEngine(), records, progressEvery));
}
else
{
    throw new ArgumentException(
        $"Unknown benchmark scenario '{benchmarkScenario}'. Use complex-vip, simple-exposure, or flat-crud.");
}

if (outputFormat == "csv")
{
    Console.Write(BenchmarkReportFormatter.ToCsv(CsvScenarioLabel(benchmarkScenario), results));
}
else
{
    Console.Write(BenchmarkReportFormatter.ToMarkdown(results));
}

static string CsvScenarioLabel(string scenario) => scenario.ToLowerInvariant() switch
{
    "flat-crud" => "Flat_CRUD",
    "deep-document" => "Deep_Document",
    "vector-search" => "Vector_Search",
    _ => scenario,
};

static async Task<BenchmarkMetrics> RunSimpleAsync(IBettingRiskEngine engine, BettingRiskScenario scenario, int iterations, int progressEvery)
{
    await using (engine)
    {
        Console.Error.WriteLine($"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: preloading {scenario.InitialOrderCount:N0} baseline orders...");

        TextWriter originalOut = Console.Out;
        Console.SetOut(TextWriter.Null);
        try
        {
            await engine.InitializeAsync(scenario);

            Console.Error.WriteLine($"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: measuring {iterations:N0} insert+read iterations...");

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            double[] iterationLatenciesMs = new double[iterations];
            long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
            long sequence = scenario.InitialOrderCount + 1L;
            var totalStopwatch = Stopwatch.StartNew();

            for (int i = 0; i < iterations; i++)
            {
                long iterationStart = Stopwatch.GetTimestamp();
                MarketTick tick = BenchmarkTickFactory.CreateLiveTick(sequence + i, scenario);
                await engine.IngestTickAsync(tick);
                _ = await engine.QueryRiskAsync(new RiskQuery(AccountId: tick.AccountId, RunnerId: tick.RunnerId));
                long iterationElapsed = Stopwatch.GetTimestamp() - iterationStart;
                iterationLatenciesMs[i] = iterationElapsed * 1000d / Stopwatch.Frequency;

                int completed = i + 1;
                if (progressEvery > 0 && (completed % progressEvery == 0 || completed == iterations))
                {
                    Console.Error.WriteLine(
                        $"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: {completed:N0}/{iterations:N0} iterations, elapsed {totalStopwatch.Elapsed.TotalSeconds:N1}s");
                }
            }

            totalStopwatch.Stop();
            long allocatedBytes = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;
            (double p50, double p99) = BenchmarkMetricsCalculator.CalculatePercentiles(iterationLatenciesMs);

            return new BenchmarkMetrics(
                engine.Name,
                totalStopwatch.Elapsed.TotalMilliseconds,
                p50,
                p99,
                allocatedBytes / 1024d / 1024d);
        }
        finally
        {
            Console.SetOut(originalOut);
        }
    }
}

static async Task<BenchmarkMetrics> RunComplexAsync(IComplexVipExposureEngine engine, ComplexVipExposureScenario scenario, int iterations, int progressEvery)
{
    await using (engine)
    {
        Console.Error.WriteLine($"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: preloading {scenario.InitialOrderCount:N0} orders, {scenario.AccountCount:N0} accounts, {scenario.MarketCount:N0} markets...");

        TextWriter originalOut = Console.Out;
        Console.SetOut(TextWriter.Null);
        try
        {
            await engine.InitializeAsync(scenario);

            Console.Error.WriteLine($"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: measuring {iterations:N0} complex JOIN+GROUP BY insert+read iterations...");

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            double[] iterationLatenciesMs = new double[iterations];
            long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
            long sequence = scenario.InitialOrderCount + 1L;
            var totalStopwatch = Stopwatch.StartNew();

            for (int i = 0; i < iterations; i++)
            {
                long iterationStart = Stopwatch.GetTimestamp();
                ComplexOrderTick order = ComplexVipTickFactory.CreateOrder(sequence + i, scenario);
                await engine.IngestOrderAsync(order);
                _ = await engine.QueryExposureAsync();
                long iterationElapsed = Stopwatch.GetTimestamp() - iterationStart;
                iterationLatenciesMs[i] = iterationElapsed * 1000d / Stopwatch.Frequency;

                int completed = i + 1;
                if (progressEvery > 0 && (completed % progressEvery == 0 || completed == iterations))
                {
                    Console.Error.WriteLine(
                        $"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: {completed:N0}/{iterations:N0} iterations, elapsed {totalStopwatch.Elapsed.TotalSeconds:N1}s");
                }
            }

            totalStopwatch.Stop();
            long allocatedBytes = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;
            (double p50, double p99) = BenchmarkMetricsCalculator.CalculatePercentiles(iterationLatenciesMs);

            return new BenchmarkMetrics(
                engine.Name,
                totalStopwatch.Elapsed.TotalMilliseconds,
                p50,
                p99,
                allocatedBytes / 1024d / 1024d);
        }
        finally
        {
            Console.SetOut(originalOut);
        }
    }
}

static BenchmarkMetrics RunFlatCrud(IFlatCrudEngine engine, int records, int progressEvery)
{
    using (engine)
    {
        Console.Error.WriteLine($"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: flat CRUD — inserting {records:N0} records then {records:N0} point lookups...");

        // Build the dataset BEFORE the measured region so record construction is not attributed to any engine.
        var data = new FlatRecord[records];
        for (int i = 0; i < records; i++)
        {
            data[i] = new FlatRecord(i + 1, $"name-{i}", i, i * 1.5d);
        }

        TextWriter originalOut = Console.Out;
        Console.SetOut(TextWriter.Null);
        try
        {
            engine.Initialize();

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            double[] lookupLatenciesMs = new double[records];
            long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
            var totalStopwatch = Stopwatch.StartNew();

            // Phase 1 — insert (batched so per-write-commit engines aren't penalized).
            engine.BeginBatch();
            for (int i = 0; i < records; i++)
            {
                engine.Insert(data[i]);

                int inserted = i + 1;
                if (progressEvery > 0 && (inserted % progressEvery == 0 || inserted == records))
                {
                    Console.Error.WriteLine(
                        $"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: inserted {inserted:N0}/{records:N0}, elapsed {totalStopwatch.Elapsed.TotalSeconds:N1}s");
                }
            }

            engine.CompleteBatch();
            Console.Error.WriteLine(
                $"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: insert phase complete in {totalStopwatch.Elapsed.TotalSeconds:N1}s; starting {records:N0} lookups...");

            // Phase 2 — point lookup by id (per-op latency captured for P99).
            long checksum = 0;
            for (int i = 0; i < records; i++)
            {
                long id = i + 1;
                long iterationStart = Stopwatch.GetTimestamp();
                FlatRecord? found = engine.GetById(id);
                lookupLatenciesMs[i] = (Stopwatch.GetTimestamp() - iterationStart) * 1000d / Stopwatch.Frequency;
                if (found is not null)
                {
                    checksum += found.Value;
                }

                int completed = i + 1;
                if (progressEvery > 0 && (completed % progressEvery == 0 || completed == records))
                {
                    Console.Error.WriteLine(
                        $"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: {completed:N0}/{records:N0} lookups, elapsed {totalStopwatch.Elapsed.TotalSeconds:N1}s");
                }
            }

            totalStopwatch.Stop();
            long allocatedBytes = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;
            (double p50, double p99) = BenchmarkMetricsCalculator.CalculatePercentiles(lookupLatenciesMs);

            // Guard against dead-code elimination of the lookups.
            if (checksum == long.MinValue)
            {
                Console.Error.WriteLine(checksum);
            }

            return new BenchmarkMetrics(
                engine.Name,
                totalStopwatch.Elapsed.TotalMilliseconds,
                p50,
                p99,
                allocatedBytes / 1024d / 1024d);
        }
        finally
        {
            Console.SetOut(originalOut);
        }
    }
}

static int ReadIntArg(string[] args, string name, int defaultValue)
{
    int index = Array.IndexOf(args, name);
    if (index < 0 || index + 1 >= args.Length)
    {
        return defaultValue;
    }

    return int.TryParse(args[index + 1], out int value) && value > 0 ? value : defaultValue;
}

static string ReadStringArg(string[] args, string name, string defaultValue)
{
    int index = Array.IndexOf(args, name);
    if (index < 0 || index + 1 >= args.Length)
    {
        return defaultValue;
    }

    return string.IsNullOrWhiteSpace(args[index + 1]) ? defaultValue : args[index + 1];
}

static bool ShouldRun(string filter, string engine) => filter is "all" || filter == engine;
