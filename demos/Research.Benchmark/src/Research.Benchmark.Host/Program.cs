using System.Diagnostics;
using DataVo.Core.StorageEngine.Config;
using Research.Benchmark.Abstractions;
using Research.Benchmark.Host;
using Research.Benchmark.Runners;
using Research.Benchmark.Runners.ComplexVip;
using Research.Benchmark.Runners.ConcurrentOps;
using Research.Benchmark.Runners.DataVo;
using Research.Benchmark.Runners.DeepDocument;
using Research.Benchmark.Runners.DiskCrud;
using Research.Benchmark.Runners.DuckDb;
using Research.Benchmark.Runners.FlatCrud;
using Research.Benchmark.Runners.Sqlite;
using Research.Benchmark.Runners.VectorSearch;

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
    if (ShouldRun(engineFilter, "sqlite"))
        results.Add(RunFlatCrud(new SqliteFlatCrudEngine(), records, progressEvery));
}
else if (benchmarkScenario.Equals("disk-crud-wal", StringComparison.OrdinalIgnoreCase))
{
    int records = ReadIntArg(args, "--records", 50_000);
    int writerWorkers = Math.Max(1, ReadIntArg(args, "--writers", 1));
    // Optional override for the GroupCommit background checkpoint cadence. A very large value parks the
    // checkpointer for the measured window (Phase-3-like), enabling a same-session checkpointer-overhead A/B.
    int checkpointIntervalArg = ReadIntArg(args, "--checkpoint-interval-ms", -1);
    int? checkpointIntervalMs = checkpointIntervalArg > 0 ? checkpointIntervalArg : null;
    // --legacy-update forces the legacy dictionary update path (disables the zero-alloc byte-patch fast path),
    // so the same session can A/B the update-phase allocation and CPU cost against the zero-alloc default.
    bool zeroAllocUpdate = !HasFlag(args, "--legacy-update");
    string root = Path.Combine(Path.GetTempPath(), $"datavo-disk-crud-wal-{Guid.NewGuid():N}");

    try
    {
        BenchmarkMetrics RunDiskScenario(IDiskCrudEngine engine) =>
            writerWorkers == 1
                ? RunDiskCrud(engine, records, progressEvery, root)
                : RunDiskCrudConcurrent(engine, records, writerWorkers, progressEvery, root);

        // Durability matrix: each DataVo mode has a like-for-like SQLite counterpart.
        //   not power-durable: DataVo (Disk)        <-> SQLite (WAL, synchronous=NORMAL)
        //   power-durable:     DataVo (Disk+fsync)  <-> SQLite (WAL, synchronous=FULL)
        if (ShouldRun(engineFilter, "datavo"))
            results.Add(RunDiskScenario(new DataVoDiskCrudEngine(durable: false)));
        if (ShouldRun(engineFilter, "datavo-pooled"))
            results.Add(RunDiskScenario(new DataVoDiskCrudEngine(durable: false, IoSchedulerMode.PoolingOnly)));
        if (ShouldRun(engineFilter, "datavo-groupcommit"))
            results.Add(RunDiskScenario(new DataVoDiskCrudEngine(durable: false, IoSchedulerMode.GroupCommit, checkpointIntervalMs, zeroAllocUpdate)));
        if (ShouldRun(engineFilter, "sqlite"))
            results.Add(RunDiskScenario(new SqliteDiskCrudEngine("NORMAL")));
        if (ShouldRun(engineFilter, "datavo-fsync"))
            results.Add(RunDiskScenario(new DataVoDiskCrudEngine(durable: true)));
        if (ShouldRun(engineFilter, "datavo-pooled-fsync"))
            results.Add(RunDiskScenario(new DataVoDiskCrudEngine(durable: true, IoSchedulerMode.PoolingOnly)));
        if (ShouldRun(engineFilter, "datavo-groupcommit-fsync"))
            results.Add(RunDiskScenario(new DataVoDiskCrudEngine(durable: true, IoSchedulerMode.GroupCommit, checkpointIntervalMs)));
        if (ShouldRun(engineFilter, "sqlite-full"))
            results.Add(RunDiskScenario(new SqliteDiskCrudEngine("FULL")));
    }
    finally
    {
        if (Directory.Exists(root))
        {
            try { Directory.Delete(root, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }
}
else if (benchmarkScenario.Equals("deep-document", StringComparison.OrdinalIgnoreCase))
{
    int orders = ReadIntArg(args, "--orders", 5_000);

    if (ShouldRun(engineFilter, "datavo"))
        results.Add(RunDeepDocument(new DataVoDeepDocumentEngine(), orders, progressEvery));
    if (ShouldRun(engineFilter, "litedb"))
        results.Add(RunDeepDocument(new LiteDbDeepDocumentEngine(), orders, progressEvery));
    if (ShouldRun(engineFilter, "sqlite"))
        results.Add(RunDeepDocument(new SqliteDeepDocumentEngine(), orders, progressEvery));
}
else if (benchmarkScenario.Equals("concurrent-ops", StringComparison.OrdinalIgnoreCase))
{
    var options = new ConcurrentOpsOptions(
        InitialRecords: ReadIntArg(args, "--records", ConcurrentOpsOptions.Default.InitialRecords),
        Duration: TimeSpan.FromSeconds(ReadIntArg(args, "--duration-seconds", (int)ConcurrentOpsOptions.Default.Duration.TotalSeconds)),
        ReaderWorkers: ReadIntArg(args, "--readers", ConcurrentOpsOptions.Default.ReaderWorkers),
        WriterWorkers: ReadIntArg(args, "--writers", ConcurrentOpsOptions.Default.WriterWorkers),
        BusyTimeout: TimeSpan.FromSeconds(ReadIntArg(args, "--busy-timeout-seconds", (int)ConcurrentOpsOptions.Default.BusyTimeout.TotalSeconds)));

    if (ShouldRun(engineFilter, "datavo"))
        results.Add(await RunConcurrentOps(new DataVoConcurrentOpsEngine(), options));
    if (ShouldRun(engineFilter, "sqlite"))
        results.Add(await RunConcurrentOps(new SqliteConcurrentOpsEngine(), options));
}
else if (benchmarkScenario.Equals("vector-search", StringComparison.OrdinalIgnoreCase))
{
    int vectors = ReadIntArg(args, "--vectors", 10_000);
    int dimensions = ReadIntArg(args, "--dimensions", 1_536);
    int queries = ReadIntArg(args, "--queries", 100);
    int topK = ReadIntArg(args, "--topk", 10);

    if (ShouldRun(engineFilter, "datavo"))
        results.Add(RunVectorSearch(new DataVoVectorSearchEngine(), vectors, dimensions, queries, topK, progressEvery));
    if (ShouldRun(engineFilter, "datavo-flat"))
        results.Add(RunVectorSearch(new DataVoVectorSearchEngine("FLAT", "DataVo-Flat", vectors), vectors, dimensions, queries, topK, progressEvery));
    if (ShouldRun(engineFilter, "litedb"))
        results.Add(RunVectorSearch(new LiteDbVectorSearchEngine(), vectors, dimensions, queries, topK, progressEvery));
    if (ShouldRun(engineFilter, "sqlite"))
        results.Add(RunVectorSearch(new SqliteVectorSearchEngine(), vectors, dimensions, queries, topK, progressEvery));
}
else
{
    throw new ArgumentException(
        $"Unknown benchmark scenario '{benchmarkScenario}'. Use complex-vip, simple-exposure, flat-crud, disk-crud-wal, deep-document, concurrent-ops, or vector-search.");
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
    "disk-crud-wal" => "Disk_CRUD_WAL",
    "deep-document" => "Deep_Document",
    "concurrent-ops" => "Concurrent_Ops",
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
            var totalStopwatch = Stopwatch.StartNew();

            // Phase 1 — insert (batched so per-write-commit engines aren't penalized).
            long insertAllocatedBefore = GC.GetAllocatedBytesForCurrentThread();
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
            long insertAllocatedBytes = GC.GetAllocatedBytesForCurrentThread() - insertAllocatedBefore;
            Console.Error.WriteLine(
                $"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: insert phase complete in {totalStopwatch.Elapsed.TotalSeconds:N1}s; starting {records:N0} lookups...");

            // Phase 2 — point lookup by id (per-op latency captured for P99).
            long checksum = 0;
            long lookupAllocatedBefore = GC.GetAllocatedBytesForCurrentThread();
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
            long lookupAllocatedBytes = GC.GetAllocatedBytesForCurrentThread() - lookupAllocatedBefore;

            totalStopwatch.Stop();
            long allocatedBytes = insertAllocatedBytes + lookupAllocatedBytes;
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
                allocatedBytes / 1024d / 1024d,
                insertAllocatedBytes / 1024d / 1024d,
                lookupAllocatedBytes / 1024d / 1024d);
        }
        finally
        {
            Console.SetOut(originalOut);
        }
    }
}

static BenchmarkMetrics RunDiskCrud(IDiskCrudEngine engine, int records, int progressEvery, string rootDir)
{
    using (engine)
    {
        Console.Error.WriteLine(
            $"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: disk CRUD (WAL) — inserting {records:N0} records then {records:N0} durable point updates...");

        // Build the dataset BEFORE the measured region so record construction is not attributed to any engine.
        var data = new FlatRecord[records];
        for (int i = 0; i < records; i++)
        {
            data[i] = new FlatRecord(i + 1, $"name-{i}", i, i * 1.5d);
        }

        string workingDir = Path.Combine(rootDir, SanitizeEngineDir(engine.Name));

        TextWriter originalOut = Console.Out;
        Console.SetOut(TextWriter.Null);
        try
        {
            engine.Initialize(workingDir);

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            double[] updateLatenciesMs = new double[records];
            var totalStopwatch = Stopwatch.StartNew();

            // Phase 1 — bulk insert inside a single batch/transaction (one durable flush amortizes the load).
            long insertAllocatedBefore = GC.GetAllocatedBytesForCurrentThread();
            var insertStopwatch = Stopwatch.StartNew();
            engine.BeginInsertBatch();
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

            engine.CompleteInsertBatch();
            insertStopwatch.Stop();
            long insertAllocatedBytes = GC.GetAllocatedBytesForCurrentThread() - insertAllocatedBefore;
            Console.Error.WriteLine(
                $"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: insert phase complete in {insertStopwatch.Elapsed.TotalSeconds:N2}s; starting {records:N0} point updates...");

            // Phase 2 — individual durable point updates (each is its own autocommit write → real disk flushes).
            long updateAllocatedBefore = GC.GetAllocatedBytesForCurrentThread();
            var updateStopwatch = Stopwatch.StartNew();
            for (int i = 0; i < records; i++)
            {
                long id = i + 1;
                int newValue = records - i;
                double newScore = newValue * 0.5d;

                long iterationStart = Stopwatch.GetTimestamp();
                engine.Update(id, newValue, newScore);
                updateLatenciesMs[i] = (Stopwatch.GetTimestamp() - iterationStart) * 1000d / Stopwatch.Frequency;

                int completed = i + 1;
                if (progressEvery > 0 && (completed % progressEvery == 0 || completed == records))
                {
                    Console.Error.WriteLine(
                        $"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: {completed:N0}/{records:N0} updates, elapsed {totalStopwatch.Elapsed.TotalSeconds:N1}s");
                }
            }

            updateStopwatch.Stop();
            long updateAllocatedBytes = GC.GetAllocatedBytesForCurrentThread() - updateAllocatedBefore;
            totalStopwatch.Stop();

            (double p50, double p99) = BenchmarkMetricsCalculator.CalculatePercentiles(updateLatenciesMs);
            long allocatedBytes = insertAllocatedBytes + updateAllocatedBytes;

            double updateBytesPerOp = records > 0 ? (double)updateAllocatedBytes / records : 0d;
            double updateMicrosPerOp = records > 0 ? updateStopwatch.Elapsed.TotalMilliseconds * 1000d / records : 0d;
            Console.Error.WriteLine(
                $"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: insert {insertStopwatch.Elapsed.TotalSeconds:N2}s | update {updateStopwatch.Elapsed.TotalSeconds:N2}s | update p50 {p50:F4}ms p99 {p99:F4}ms");
            Console.Error.WriteLine(
                $"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: UPDATE alloc {updateAllocatedBytes / 1024d / 1024d:N2} MB total = {updateBytesPerOp:N1} B/op | UPDATE cpu {updateMicrosPerOp:N2} us/op | insert alloc {insertAllocatedBytes / 1024d / 1024d:N2} MB");

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

static BenchmarkMetrics RunDiskCrudConcurrent(IDiskCrudEngine engine, int records, int writers, int progressEvery, string rootDir)
{
    using (engine)
    {
        Console.Error.WriteLine(
            $"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: concurrent disk CRUD (WAL) — inserting {records:N0} records then {records:N0} point updates across {writers:N0} writers...");

        var data = new FlatRecord[records];
        for (int i = 0; i < records; i++)
        {
            data[i] = new FlatRecord(i + 1, $"name-{i}", i, i * 1.5d);
        }

        string workingDir = Path.Combine(rootDir, SanitizeEngineDir(engine.Name));

        TextWriter originalOut = Console.Out;
        Console.SetOut(TextWriter.Null);
        try
        {
            engine.Initialize(workingDir);

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            double[] updateLatenciesMs = new double[records];
            long allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
            var totalStopwatch = Stopwatch.StartNew();

            var insertStopwatch = Stopwatch.StartNew();
            engine.BeginInsertBatch();
            for (int i = 0; i < records; i++)
            {
                engine.Insert(data[i]);
            }

            engine.CompleteInsertBatch();
            insertStopwatch.Stop();

            Console.Error.WriteLine(
                $"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: insert phase complete in {insertStopwatch.Elapsed.TotalSeconds:N2}s; starting concurrent updates...");

            long nextIndex = -1;
            int completed = 0;
            var updateStopwatch = Stopwatch.StartNew();
            Task[] tasks = new Task[writers];
            for (int worker = 0; worker < writers; worker++)
            {
                tasks[worker] = Task.Run(() =>
                {
                    while (true)
                    {
                        long index = Interlocked.Increment(ref nextIndex);
                        if (index >= records)
                        {
                            return;
                        }

                        long id = index + 1;
                        int newValue = records - (int)index;
                        double newScore = newValue * 0.5d;

                        long iterationStart = Stopwatch.GetTimestamp();
                        engine.Update(id, newValue, newScore);
                        updateLatenciesMs[index] = (Stopwatch.GetTimestamp() - iterationStart) * 1000d / Stopwatch.Frequency;

                        int current = Interlocked.Increment(ref completed);
                        if (progressEvery > 0 && (current % progressEvery == 0 || current == records))
                        {
                            Console.Error.WriteLine(
                                $"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: {current:N0}/{records:N0} concurrent updates, elapsed {totalStopwatch.Elapsed.TotalSeconds:N1}s");
                        }
                    }
                });
            }

            Task.WaitAll(tasks);
            updateStopwatch.Stop();
            totalStopwatch.Stop();

            long allocatedBytes = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
            (double p50, double p99) = BenchmarkMetricsCalculator.CalculatePercentiles(updateLatenciesMs);

            Console.Error.WriteLine(
                $"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: insert {insertStopwatch.Elapsed.TotalSeconds:N2}s | concurrent update {updateStopwatch.Elapsed.TotalSeconds:N2}s | update p50 {p50:F4}ms p99 {p99:F4}ms");

            return new BenchmarkMetrics(
                $"{engine.Name} ({writers} writers)",
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

static string SanitizeEngineDir(string engineName)
{
    var chars = engineName.Select(c => char.IsLetterOrDigit(c) ? c : '_').ToArray();
    return new string(chars);
}

static BenchmarkMetrics RunDeepDocument(IDeepDocumentEngine engine, int orders, int progressEvery)
{
    using (engine)
    {
        Console.Error.WriteLine($"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: deep document — saving {orders:N0} nested orders (5 items, 2 addresses each) then loading {orders:N0}...");

        // Build the dataset BEFORE the measured region.
        var data = new DeepOrder[orders];
        for (int i = 0; i < orders; i++)
        {
            data[i] = BuildDeepOrder(i + 1);
        }

        TextWriter originalOut = Console.Out;
        Console.SetOut(TextWriter.Null);
        try
        {
            engine.Initialize();

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            double[] loadLatenciesMs = new double[orders];
            long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
            var totalStopwatch = Stopwatch.StartNew();

            // Phase 1 — save nested orders.
            engine.BeginBatch();
            for (int i = 0; i < orders; i++)
            {
                engine.Save(data[i]);

                int saved = i + 1;
                if (progressEvery > 0 && (saved % progressEvery == 0 || saved == orders))
                {
                    Console.Error.WriteLine(
                        $"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: saved {saved:N0}/{orders:N0}, elapsed {totalStopwatch.Elapsed.TotalSeconds:N1}s");
                }
            }

            engine.CompleteBatch();
            Console.Error.WriteLine(
                $"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: save phase complete in {totalStopwatch.Elapsed.TotalSeconds:N1}s; starting {orders:N0} loads...");

            // Phase 2 — load + reconstruct each order whole (per-op latency for P99).
            long checksum = 0;
            for (int i = 0; i < orders; i++)
            {
                long iterationStart = Stopwatch.GetTimestamp();
                DeepOrder? loaded = engine.Load(i + 1);
                loadLatenciesMs[i] = (Stopwatch.GetTimestamp() - iterationStart) * 1000d / Stopwatch.Frequency;
                if (loaded is not null)
                {
                    checksum += loaded.Items.Count + loaded.Addresses.Count;
                }

                int done = i + 1;
                if (progressEvery > 0 && (done % progressEvery == 0 || done == orders))
                {
                    Console.Error.WriteLine(
                        $"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: {done:N0}/{orders:N0} loads, elapsed {totalStopwatch.Elapsed.TotalSeconds:N1}s");
                }
            }

            totalStopwatch.Stop();
            long allocatedBytes = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;
            (double p50, double p99) = BenchmarkMetricsCalculator.CalculatePercentiles(loadLatenciesMs);

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

static async Task<BenchmarkMetrics> RunConcurrentOps(IConcurrentOpsEngine engine, ConcurrentOpsOptions options)
{
    await using (engine)
    {
        Console.Error.WriteLine(
            $"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: concurrent ops — preloading {options.InitialRecords:N0} records, then {options.ReaderWorkers} readers + {options.WriterWorkers} writers for {options.Duration.TotalSeconds:N0}s...");

        TextWriter originalOut = Console.Out;
        Console.SetOut(TextWriter.Null);
        try
        {
            await engine.InitializeAsync(options);

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            long allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
            ConcurrentOpsResult result = await engine.RunAsync(options);
            long allocatedBytes = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;

            Console.Error.WriteLine(
                $"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: {result.ReadOperations:N0} reads + {result.WriteOperations:N0} writes, {result.TotalOperationsPerSecond:N0} OPS");

            return new BenchmarkMetrics(
                engine.Name,
                result.ElapsedMs,
                0d,
                0d,
                allocatedBytes / 1024d / 1024d,
                OpsPerSecond: result.TotalOperationsPerSecond,
                ReadP99LatencyMs: result.ReadP99LatencyMs,
                WriteP99LatencyMs: result.WriteP99LatencyMs);
        }
        finally
        {
            Console.SetOut(originalOut);
        }
    }
}

static BenchmarkMetrics RunVectorSearch(IVectorSearchEngine engine, int vectors, int dimensions, int queries, int topK, int progressEvery)
{
    using (engine)
    {
        Console.Error.WriteLine($"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: vector search — inserting {vectors:N0} x {dimensions}-dim vectors then {queries:N0} top-{topK} queries...");

        // Build the dataset + query set BEFORE the measured region (deterministic RNG for reproducibility).
        var rng = new Random(20260623);
        var data = new float[vectors][];
        for (int i = 0; i < vectors; i++)
        {
            data[i] = RandomUnitVector(rng, dimensions);
        }

        var queryVectors = new float[queries][];
        for (int q = 0; q < queries; q++)
        {
            queryVectors[q] = data[(q * 97) % vectors]; // query with existing vectors spread across the set
        }

        try
        {
            engine.Initialize(dimensions);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: unavailable — {ex.Message} (reported as n/a)");
            return new BenchmarkMetrics(engine.Name, double.NaN, double.NaN, double.NaN, double.NaN);
        }

        TextWriter originalOut = Console.Out;
        Console.SetOut(TextWriter.Null);
        try
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            double[] queryLatenciesMs = new double[queries];
            long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
            var totalStopwatch = Stopwatch.StartNew();

            // Phase 1 — insert vectors.
            engine.BeginBatch();
            for (int i = 0; i < vectors; i++)
            {
                engine.Insert(i + 1, data[i]);

                int inserted = i + 1;
                if (progressEvery > 0 && (inserted % progressEvery == 0 || inserted == vectors))
                {
                    Console.Error.WriteLine(
                        $"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: inserted {inserted:N0}/{vectors:N0}, elapsed {totalStopwatch.Elapsed.TotalSeconds:N1}s");
                }
            }

            engine.CompleteBatch();
            Console.Error.WriteLine(
                $"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: insert phase complete in {totalStopwatch.Elapsed.TotalSeconds:N1}s; starting {queries:N0} top-{topK} queries...");

            // Phase 2 — Top-K nearest-neighbour queries (per-query latency for P99).
            long sink = 0;
            for (int q = 0; q < queries; q++)
            {
                long iterationStart = Stopwatch.GetTimestamp();
                IReadOnlyList<long> ids = engine.Search(queryVectors[q], topK);
                queryLatenciesMs[q] = (Stopwatch.GetTimestamp() - iterationStart) * 1000d / Stopwatch.Frequency;
                sink += ids.Count;

                int done = q + 1;
                if (progressEvery > 0 && (done % Math.Max(1, progressEvery / 100) == 0 || done == queries))
                {
                    Console.Error.WriteLine(
                        $"[{DateTimeOffset.Now:HH:mm:ss}] {engine.Name}: {done:N0}/{queries:N0} queries, elapsed {totalStopwatch.Elapsed.TotalSeconds:N1}s");
                }
            }

            totalStopwatch.Stop();
            long allocatedBytes = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;
            (double p50, double p99) = BenchmarkMetricsCalculator.CalculatePercentiles(queryLatenciesMs);

            if (sink == long.MinValue)
            {
                Console.Error.WriteLine(sink);
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

static float[] RandomUnitVector(Random rng, int dimensions)
{
    var vector = new float[dimensions];
    double sumSquares = 0d;
    for (int i = 0; i < dimensions; i++)
    {
        float value = (float)(rng.NextDouble() * 2d - 1d);
        vector[i] = value;
        sumSquares += value * (double)value;
    }

    float norm = (float)Math.Sqrt(sumSquares);
    if (norm > 0f)
    {
        for (int i = 0; i < dimensions; i++)
        {
            vector[i] /= norm;
        }
    }

    return vector;
}

static DeepOrder BuildDeepOrder(long id)
{
    var items = new OrderItem[5];
    for (int s = 0; s < items.Length; s++)
    {
        items[s] = new OrderItem(Sku: (int)(id * 10 + s), Name: $"item-{s}", Quantity: s + 1, UnitPrice: (s + 1) * 9.99d);
    }

    var addresses = new OrderAddress[]
    {
        new("billing", $"{id} Main St", "Springfield", "12345"),
        new("shipping", $"{id} Oak Ave", "Shelbyville", "67890"),
    };

    double total = items.Sum(item => item.Quantity * item.UnitPrice);
    return new DeepOrder(id, $"cust-{id}", total, items, addresses);
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

static bool HasFlag(string[] args, string name) =>
    Array.IndexOf(args, name) >= 0;

static bool ShouldRun(string filter, string engine) => filter is "all" || filter == engine;
