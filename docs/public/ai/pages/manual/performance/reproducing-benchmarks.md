# Reproducing Benchmarks

> Source route: /manual/performance/reproducing-benchmarks
> Source file: manual/performance/reproducing-benchmarks.md

Every chart and table in the performance documentation can be reproduced from the repository root using Release builds on .NET 10. Run scenarios **serially** on an idle machine; parallel runs contend on CPU and storage flushes and invalidate latency tails.

```bash
# 1. Capture environment metadata first
git rev-parse HEAD && dotnet --info && uname -a

# 2. Build the research benchmark host once
dotnet build demos/Research.Benchmark/src/Research.Benchmark.Host -c Release
DLL=demos/Research.Benchmark/src/Research.Benchmark.Host/bin/Release/net10.0/Research.Benchmark.Host.dll
```

## 1. SQL Matrix & Hybrid Vector (`DataVo vs SQLite`)

Reproduces the 28-query 1,000,000-row SQL comparison and 5 filtered nearest-neighbour shapes in [DataVo vs SQLite](/manual/performance/datavo-vs-sqlite).

```bash
# 28 relational SQL shapes over 1,000,000 orders (~2 min, ~1.2 GB peak RAM)
dotnet "$DLL" --scenario sql-matrix > sql-matrix.md 2> sql-matrix.log

# Filtered vector search (50k × 384d and 20k × 1,536d; requires SQLITE_VEC_PATH)
SQLITE_VEC_PATH=$PWD/vec0.dylib dotnet "$DLL" --scenario hybrid-vector > hybrid-vector.md
SQLITE_VEC_PATH=$PWD/vec0.dylib dotnet "$DLL" --scenario hybrid-vector --vectors 20000 --dimensions 1536 > hybrid-vector-1536.md
```

Useful filters: `--records 100000` (fast 1-minute smoke), `--queries J5,C8` (specific shapes), `--engine datavo,datavo-mem,sqlite,duckdb` (or `datavo-flat,datavo-hnsw,sqlite`).

::: details Installing `sqlite-vec` v0.1.9 (`SQLITE_VEC_PATH`)
**macOS arm64:**
```bash
curl -sL -o /tmp/svec.tgz https://github.com/asg017/sqlite-vec/releases/download/v0.1.9/sqlite-vec-0.1.9-loadable-macos-aarch64.tar.gz
tar xzf /tmp/svec.tgz -C /tmp && export SQLITE_VEC_PATH=/tmp/vec0.dylib
```

**Linux x64:**
```bash
curl -sL -o /tmp/svec.tgz https://github.com/asg017/sqlite-vec/releases/download/v0.1.9/sqlite-vec-0.1.9-loadable-linux-x86_64.tar.gz
tar xzf /tmp/svec.tgz -C /tmp && export SQLITE_VEC_PATH=/tmp/vec0.so
```

**Windows x64 (PowerShell):**
```powershell
curl.exe -L -o $env:TEMP\svec.tgz https://github.com/asg017/sqlite-vec/releases/download/v0.1.9/sqlite-vec-0.1.9-loadable-windows-x86_64.tar.gz
tar -xzf $env:TEMP\svec.tgz -C $env:TEMP
$env:SQLITE_VEC_PATH = "$env:TEMP\vec0.dll"
```
:::

## 2. BenchmarkDotNet Microbenchmarks (Part I)

Runs the statistical microbenchmark suite (`benchmarks/DataVo.Benchmarks`, ~15 minutes) comparing DataVo, SQLite, and DuckDB on identical data. Outputs Markdown, CSV, and HTML reports to `BenchmarkDotNet.Artifacts/results/`.

```bash
dotnet run -c Release --project benchmarks/DataVo.Benchmarks -- --filter '*'
```

## 3. Systems Workloads (Part II)

| Scenario | Command | Notes |
| --- | --- | --- |
| **Flat CRUD** | `dotnet "$DLL" --scenario flat-crud --format markdown` | 50k inserts + 50k in-memory lookups |
| **Disk CRUD WAL** | `dotnet "$DLL" --scenario disk-crud-wal --records 20000 --format markdown` | ~3 min (20k strict `fsync` commits) |
| **Thread Scaling** | `dotnet "$DLL" --scenario thread-scaling --engine datavo-lsm-relaxed --format markdown` | Use `--engine all` to include strict mode |
| **YCSB Mixed 80/20** | `dotnet "$DLL" --scenario ycsb-mixed --records 100000 --format markdown` | Multi-threaded read/write tail latency |
| **Space & Recovery** | `dotnet "$DLL" --scenario space-and-recovery --records 1000000 --format markdown` | 1M rows, disk size & warm reopen |
| **Vector Search** | `dotnet "$DLL" --scenario vector-search --vectors 10000 --dimensions 1536 --queries 100 --topk 10 --format markdown` | Requires `SQLITE_VEC_PATH` |
| **HNSW vs. Flat Recall** | `dotnet run -c Release --project demos/Research.Benchmark/tools/VectorRecall` | Held-out recall@10 (~1 min) |
| **Reactive IVM** | `dotnet "$DLL" --scenario simple-exposure --format markdown` and `--scenario complex-vip` | Single-table & 3-table Z-set IVM |
| **Other Workloads** | `--scenario deep-document`, `--scenario concurrent-ops`, `--scenario sql-workload` | App-shaped & 1M-row SQL workloads |

Always keep durability settings explicit when comparing strict vs. relaxed LSM:

```csharp
var strict  = new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = "./bench_strict",  LsmStrictFsync = true };
var relaxed = new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = "./bench_relaxed", LsmStrictFsync = false };
```

## 4. Arena Live Views, Unity IL2CPP & Charts

::: details Arena Live Views Matrix (Desktop .NET & Unity macOS IL2CPP)
**Desktop .NET lane (7 repetitions per profile):**
```bash
dotnet "$DLL" --scenario arena-live-views --profile gameplay --engine all --repetitions 7 --output /private/tmp/datavo-arena-gameplay --format markdown
dotnet "$DLL" --scenario arena-live-views --profile ivm-stress --engine all --repetitions 7 --output /private/tmp/datavo-arena-ivm-stress --format markdown
```

**Unity macOS IL2CPP lanes (requires Unity 6000.5.4f1 + `mac-il2cpp`):**
```bash
bash scripts/check-unity-support-status.sh
bash scripts/run-unity-macos-il2cpp-player-smoke.sh
bash scripts/run-unity-macos-il2cpp-arena-cross-engine.sh
bash scripts/run-unity-arena-editor-tests.sh
bash scripts/run-unity-macos-il2cpp-arena.sh
```
*Do not compare Unity IL2CPP timings directly with desktop .NET timings; evaluate each lane independently with its canonical checksums.*
:::

::: details Regenerating Charts & CI Workflow Artifacts
**Regenerate PNG charts** (requires Python 3, `matplotlib`, `numpy`; IVM plots also require `rsvg-convert`):
```bash
python3 docs/scripts/generate-benchmark-plots.py
python3 docs/scripts/generate-ivm-benchmark-plots.py
python3 docs/scripts/generate-ci-benchmark-plots.py
```

**CI snapshot reproduction:** Trigger `.github/workflows/benchmark.yml` via `workflow_dispatch` on GitHub Actions to run the full BenchmarkDotNet + Research Matrix suites across Ubuntu x64, Windows x64, and macOS runners. Download artifacts with `gh run download <run-id>`.
:::

## See Also
- [Benchmark Results & Methodology](/manual/performance/benchmarks): Full results, methodology, and cross-platform CI findings
- [DataVo vs SQLite](/manual/performance/datavo-vs-sqlite): 1M-row SQL matrix and hybrid vector decision guide
