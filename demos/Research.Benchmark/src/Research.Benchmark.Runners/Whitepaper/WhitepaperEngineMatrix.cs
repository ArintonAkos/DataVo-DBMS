namespace Research.Benchmark.Runners.Whitepaper;

public static class WhitepaperEngineMatrix
{
    public static IReadOnlyList<IWhitepaperBenchmarkEngine> Create(string engineFilter)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(engineFilter);

        var engines = new List<IWhitepaperBenchmarkEngine>();
        bool all = string.Equals(engineFilter, "all", StringComparison.OrdinalIgnoreCase);

        if (ShouldRun(engineFilter, "datavo-lsm-production", all))
            engines.Add(new DataVoWhitepaperBenchmarkEngine(durable: true));
        if (ShouldRun(engineFilter, "datavo-lsm-relaxed", all) || ShouldRun(engineFilter, "datavo-lsm", includeInDefaultMatrix: false))
            engines.Add(new DataVoWhitepaperBenchmarkEngine(durable: false));
        if (ShouldRun(engineFilter, "sqlite", all) || ShouldRun(engineFilter, "sqlite-normal", includeInDefaultMatrix: false))
            engines.Add(new SqliteWhitepaperBenchmarkEngine("NORMAL"));
        if (ShouldRun(engineFilter, "litedb", all))
            engines.Add(new LiteDbWhitepaperBenchmarkEngine());

        return engines;
    }

    private static bool ShouldRun(string filter, string engine, bool includeInDefaultMatrix) =>
        (includeInDefaultMatrix && string.Equals(filter, "all", StringComparison.OrdinalIgnoreCase)) ||
        string.Equals(filter, engine, StringComparison.OrdinalIgnoreCase);
}
