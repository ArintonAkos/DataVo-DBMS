# Reproducing Benchmarks

> Source route: /manual/performance/reproducing-benchmarks
> Source file: manual/performance/reproducing-benchmarks.md

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

SQLite vector search requires the `sqlite-vec` loadable `vec0` extension. The GitHub Actions benchmark workflow downloads the pinned `v0.1.9` loadable release for Linux x64 and Windows x64, verifies SHA256, exports `SQLITE_VEC_PATH`, and uploads the extension artifact with the run. For local runs, set `SQLITE_VEC_PATH` yourself before launching the vector scenario.

Linux x64:

```bash
curl -sL -o /tmp/svec.tgz \
  https://github.com/asg017/sqlite-vec/releases/download/v0.1.9/sqlite-vec-0.1.9-loadable-linux-x86_64.tar.gz
tar xzf /tmp/svec.tgz -C /tmp
export SQLITE_VEC_PATH=/tmp/vec0.so
```

Windows x64, PowerShell:

```powershell
curl.exe -L -o $env:TEMP\svec.tgz `
  https://github.com/asg017/sqlite-vec/releases/download/v0.1.9/sqlite-vec-0.1.9-loadable-windows-x86_64.tar.gz
tar -xzf $env:TEMP\svec.tgz -C $env:TEMP
$env:SQLITE_VEC_PATH = "$env:TEMP\vec0.dll"
```

The thread-scaling workload is the one to run when you want to inspect the LSM Relaxed result. Use the
relaxed engine filter for that number; the default `all` matrix also runs strict production durability, which
can be much slower on Windows because it includes fsync-heavy update work.

```bash
dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host -- --scenario thread-scaling --engine datavo-lsm-relaxed --format markdown
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

## Reproducing the CI Snapshot

The Linux and Windows measurements in the benchmark results come from the GitHub Actions workflow `.github/workflows/benchmark.yml`. It runs on Ubuntu and Windows hosted runners, downloads the pinned `sqlite-vec v0.1.9` loadable extension for each OS, verifies its SHA256, exports `SQLITE_VEC_PATH`, runs the same scenario commands shown above, and uploads the results as workflow artifacts. To reproduce it, trigger that workflow from the repository's Actions tab (it supports manual `workflow_dispatch`), or run the same `dotnet run` commands on a local host with `SQLITE_VEC_PATH` pointing at the extension.

## Reproduction Support

All six scenarios above are reproducible from the documented commands: flat CRUD (in-process insert and point lookup), disk CRUD WAL (with strict/relaxed durability comparisons), vector search (10,000 vectors, 1536 dimensions, 100 queries, top-10), thread scaling (1, 2, 4, 8, 16, and 32 threads), YCSB mixed (100,000 records mixing reads and updates), and space-and-recovery (disk footprint and recovery). A long-term public CI performance gate is planned but not yet in place.
