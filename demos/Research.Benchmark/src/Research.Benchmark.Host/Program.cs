using System.Diagnostics;
using Research.Benchmark.Abstractions;
using Research.Benchmark.Host;
using Research.Benchmark.Runners;
using Research.Benchmark.Runners.ComplexVip;
using Research.Benchmark.Runners.DataVo;
using Research.Benchmark.Runners.DuckDb;
using Research.Benchmark.Runners.Sqlite;

string benchmarkScenario = ReadStringArg(args, "--scenario", "complex-vip");
int baselineOrders = ReadIntArg(args, "--baseline", 10_000);
int iterations = ReadIntArg(args, "--iterations", 50_000);
int progressEvery = ReadIntArg(args, "--progress-every", 1_000);
string engineFilter = ReadStringArg(args, "--engine", "all").ToLowerInvariant();

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
else
{
    throw new ArgumentException($"Unknown benchmark scenario '{benchmarkScenario}'. Use complex-vip or simple-exposure.");
}

Console.Write(BenchmarkReportFormatter.ToMarkdown(results));

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
