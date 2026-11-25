using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.DiskCrud;

public static class DiskCrudEngineMatrix
{
    public static IReadOnlyList<IDiskCrudEngine> Create(
        string engineFilter,
        bool zeroAllocUpdate)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(engineFilter);

        var engines = new List<IDiskCrudEngine>();
        bool runDefaultMatrix = string.Equals(engineFilter, "all", StringComparison.OrdinalIgnoreCase);

        if (ShouldRun(engineFilter, "datavo-lsm-production", runDefaultMatrix) ||
            ShouldRun(engineFilter, "datavo", includeInDefaultMatrix: false))
            engines.Add(new DataVoDiskCrudEngine(durable: true, zeroAllocUpdate));
        if (ShouldRun(engineFilter, "datavo-lsm-relaxed", runDefaultMatrix) ||
            ShouldRun(engineFilter, "datavo-lsm", includeInDefaultMatrix: false))
            engines.Add(new DataVoDiskCrudEngine(durable: false, zeroAllocUpdate));
        if (ShouldRun(engineFilter, "sqlite", runDefaultMatrix))
            engines.Add(new SqliteDiskCrudEngine("NORMAL"));
        if (ShouldRun(engineFilter, "sqlite-full", runDefaultMatrix))
            engines.Add(new SqliteDiskCrudEngine("FULL"));

        return engines;
    }

    private static bool ShouldRun(string filter, string engine, bool includeInDefaultMatrix) =>
        (includeInDefaultMatrix && string.Equals(filter, "all", StringComparison.OrdinalIgnoreCase)) ||
        string.Equals(filter, engine, StringComparison.OrdinalIgnoreCase);
}
