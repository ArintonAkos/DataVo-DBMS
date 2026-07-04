# Reproducing Benchmarks

Run benchmarks from the repository root so relative paths, artifacts, and native extension configuration resolve the same way as the checked-in benchmark host expects.

Start with a single scenario and markdown output.

```bash
dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host -- --scenario flat-crud --format markdown
```

The disk CRUD workload compares DataVo LSM strict mode, DataVo LSM relaxed mode, and SQLite WAL modes under point-update pressure.

```bash
dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host -- --scenario disk-crud-wal --records 20000 --format markdown
```

The vector workload inserts 10,000 vectors, uses 1536 dimensions, and runs 100 top-10 queries. This is the run behind the allocation story on the benchmark page.

```bash
dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host -- --scenario vector-search --vectors 10000 --dimensions 1536 --queries 100 --topk 10 --format markdown
```

The thread-scaling workload is the one to run when you want to inspect the 1.2M ops/s LSM Relaxed result.

```bash
dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host -- --scenario thread-scaling --format markdown
```

The YCSB-style mixed workload preloads records and mixes reads with updates so write-tail latency can be compared.

```bash
dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host -- --scenario ycsb-mixed --records 100000 --format markdown
```

The space and recovery workload measures insert time, on-disk size, recovery time, and managed allocation.

```bash
dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host -- --scenario space-and-recovery --records 1000000 --format markdown
```

Record the environment with every run. At minimum, capture the commit SHA, OS, CPU, .NET SDK, storage device, native SQLite extension path, storage mode, and durability setting.

```bash
git rev-parse HEAD
dotnet --info
uname -a
```

When comparing strict and relaxed LSM, keep the configuration difference visible in the report. The two modes answer different questions.

```csharp
var strict = new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./bench_strict",
    LsmStrictFsync = true
};

var relaxed = new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./bench_relaxed",
    LsmStrictFsync = false
};
```

## Reproduction Support

| Feature | Status | Notes |
| --- | --- | --- |
| Flat CRUD scenario | Supported | Compares in-process insert and point lookup paths. |
| Disk CRUD WAL scenario | Supported | Includes strict/relaxed durability comparisons. |
| Vector search scenario | Supported | Uses 10,000 vectors, 1536 dimensions, 100 queries, and top-10 results in the documented command. |
| Thread scaling scenario | Supported | Includes 1, 2, 4, 8, 16, and 32 thread runs. |
| YCSB mixed scenario | Supported | Preloads 100,000 records and mixes reads with updates. |
| Space and recovery scenario | Supported | Measures disk footprint and recovery behavior. |
| Stable public CI benchmark gate | Planned | Commands are documented; a long-term CI performance gate is future work. |
