using DataVo.Core.StorageEngine.Config;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.DiskCrud;

public static class DiskCrudEngineMatrix
{
    public static IReadOnlyList<IDiskCrudEngine> Create(
        string engineFilter,
        int? checkpointIntervalMs,
        bool zeroAllocUpdate)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(engineFilter);

        var engines = new List<IDiskCrudEngine>();
        bool runDefaultMatrix = string.Equals(engineFilter, "all", StringComparison.OrdinalIgnoreCase);

        if (ShouldRun(engineFilter, "datavo", includeInDefaultMatrix: false))
            engines.Add(new DataVoDiskCrudEngine(durable: false));
        if (ShouldRun(engineFilter, "datavo-lsm-production", runDefaultMatrix))
            engines.Add(new DataVoDiskCrudEngine(durable: true, storageMode: DataVoDiskCrudStorageMode.Lsm));
        if (ShouldRun(engineFilter, "datavo-lsm-relaxed", runDefaultMatrix) ||
            ShouldRun(engineFilter, "datavo-lsm", includeInDefaultMatrix: false))
            engines.Add(new DataVoDiskCrudEngine(durable: false, storageMode: DataVoDiskCrudStorageMode.Lsm));
        if (ShouldRun(engineFilter, "datavo-pooled", includeInDefaultMatrix: false))
            engines.Add(new DataVoDiskCrudEngine(durable: false, IoSchedulerMode.PoolingOnly));
        if (ShouldRun(engineFilter, "datavo-groupcommit", includeInDefaultMatrix: false))
            engines.Add(new DataVoDiskCrudEngine(durable: false, IoSchedulerMode.GroupCommit, checkpointIntervalMs, zeroAllocUpdate));
        if (ShouldRun(engineFilter, "sqlite", runDefaultMatrix))
            engines.Add(new SqliteDiskCrudEngine("NORMAL"));
        if (ShouldRun(engineFilter, "datavo-fsync", includeInDefaultMatrix: false))
            engines.Add(new DataVoDiskCrudEngine(durable: true));
        if (ShouldRun(engineFilter, "datavo-pooled-fsync", includeInDefaultMatrix: false))
            engines.Add(new DataVoDiskCrudEngine(durable: true, IoSchedulerMode.PoolingOnly));
        if (ShouldRun(engineFilter, "datavo-groupcommit-fsync", includeInDefaultMatrix: false))
            engines.Add(new DataVoDiskCrudEngine(durable: true, IoSchedulerMode.GroupCommit, checkpointIntervalMs));
        if (ShouldRun(engineFilter, "sqlite-full", runDefaultMatrix))
            engines.Add(new SqliteDiskCrudEngine("FULL"));

        return engines;
    }

    private static bool ShouldRun(string filter, string engine, bool includeInDefaultMatrix) =>
        (includeInDefaultMatrix && string.Equals(filter, "all", StringComparison.OrdinalIgnoreCase)) ||
        string.Equals(filter, engine, StringComparison.OrdinalIgnoreCase);
}
