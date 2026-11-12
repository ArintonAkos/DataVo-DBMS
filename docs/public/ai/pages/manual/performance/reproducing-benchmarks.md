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

macOS arm64:

```bash
curl -sL -o /tmp/svec.tgz \
  https://github.com/asg017/sqlite-vec/releases/download/v0.1.9/sqlite-vec-0.1.9-loadable-macos-aarch64.tar.gz
tar xzf /tmp/svec.tgz -C /tmp
export SQLITE_VEC_PATH=/tmp/vec0.dylib
```

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

The two incremental-view scenarios are named `simple-exposure` (profile A, one table) and `complex-vip` (profile B, a three-table join). The remaining scenarios from the results page are `deep-document`, `concurrent-ops` and `sql-workload`.

```bash
dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host -- --scenario simple-exposure --format markdown
dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host -- --scenario complex-vip --format markdown
dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host -- --scenario deep-document --format markdown
dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host -- --scenario concurrent-ops --format markdown
dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host -- --scenario sql-workload --format markdown
```

Run scenarios one at a time. Running them in parallel changes every result, and the strict-durability ones also compete for the same drive. The `disk-crud-wal` scenario takes about three minutes because two of its engines flush the drive 20,000 times.

## BenchmarkDotNet suite

The microbenchmarks in `benchmarks/DataVo.Benchmarks` compare DataVo with SQLite and DuckDB on identical data. A full run takes about 15 minutes. Close other programs first.

```bash
dotnet run -c Release --project benchmarks/DataVo.Benchmarks -- --filter '*'
```

Results are written to `BenchmarkDotNet.Artifacts/results/` as Markdown, CSV and HTML. The benchmark project's README lists what each class measures and how each engine is configured.

## HNSW recall and latency against Flat

The vector scenario reports query latency but not recall, and its query vectors are members of the data set. `demos/Research.Benchmark/tools/VectorRecall` builds Flat and HNSW indexes at 1,000, 10,000 and 50,000 vectors, on uniform and on clustered data, and measures latency and recall@10 with held-out queries. It takes about a minute.

```bash
dotnet run -c Release --project demos/Research.Benchmark/tools/VectorRecall
```

## Regenerating the charts

The charts on the results page are generated from the raw files in `docs/public/benchmarks/<run date>/`. Copy new scenario output into a new dated folder, set `RUN` in the script, and run it. It needs Python 3 with `matplotlib` and `numpy`.

```bash
python3 docs/scripts/generate-benchmark-plots.py
python3 docs/scripts/generate-ivm-benchmark-plots.py   # also needs rsvg-convert
```

The two incremental-view plots read their values from the script, not from the raw files. Update those values by hand when you rerun `simple-exposure` and `complex-vip`.

## Unity IL2CPP smoke

These need Unity 6000.5.4f1 with the macOS IL2CPP module installed. The smoke builds a macOS IL2CPP player, runs it, and checks that it reports a passing result. It is a correctness check and produces no timings.

```bash
bash scripts/check-unity-support-status.sh
bash scripts/run-unity-macos-il2cpp-player-smoke.sh
```

## Arena Live Views cross-engine matrix

The relational arena matrix measures three demanded live views after every write batch. It is separate from vector search: neither the Unity NPC-memory demo nor the desktop `vector-search` scenario belongs in this matrix. All raw JSON paths written by the commands below are part of the result.

### Desktop .NET lane

The desktop .NET table compares DataVo reactive, DataVo polling, SQLite, and DuckDB on .NET. DuckDB is desktop-only. Run the fixed profiles serially and choose output directories outside tracked source files:

```bash
dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host -- \
  --scenario arena-live-views --profile gameplay --engine all --repetitions 7 \
  --output /private/tmp/datavo-arena-gameplay --format markdown

dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host -- \
  --scenario arena-live-views --profile ivm-stress --engine all --repetitions 7 \
  --output /private/tmp/datavo-arena-ivm-stress --format markdown
```

### Unity macOS IL2CPP lane

The Unity table compares only DataVo reactive, DataVo polling, and macOS system SQLite in IL2CPP. The runner builds once, launches seven fresh Player processes for each engine/profile pair, preserves each raw JSON path, and gives every Player process a 120-second hard limit. A timeout, failed repetition, missing engine, or hash mismatch fails the command.

```bash
bash scripts/run-unity-macos-il2cpp-arena-cross-engine.sh
```

do not compare Unity IL2CPP values directly with desktop .NET values

The commands are reproduction gates, not a claim that every invocation has already passed. Interpret a lane only from its completed artifacts: seven runs per engine/profile, the median total time, within-run percentiles, and identical canonical leaderboard, team-aggregate, and zone-inventory hashes for a profile.

## Unity NPC Memory Arena

On macOS with Unity 6000.5.4f1 and the matching `mac-il2cpp` module installed, run the Editor checks and the actual Medium-stripping IL2CPP Player proof from the repository root.

```bash
bash scripts/run-unity-arena-editor-tests.sh
bash scripts/run-unity-macos-il2cpp-arena.sh
```

The Player script builds once, runs deterministic reactive and polling modes separately, writes their JSON reports under Unity's persistent-data directory, and requires their final leaderboard and aggregate checksums to match. The result compares DataVo incremental maintained views against full-query polling with the same DataVo data and SQL. It is not a DataVo-versus-SQLite benchmark; interpret it with its recorded seed, tick count, and Unity backend.

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

Every table on the [results page](./benchmarks.md) comes from one of the commands above, and the raw output of the last run is checked in under `docs/public/benchmarks/2026-11-12/`. The Linux and Windows measurements are produced by the GitHub Actions workflow, and the Linux figures kept in the repository are from 4 July 2026. A long-term public CI performance gate is planned but not yet in place.
