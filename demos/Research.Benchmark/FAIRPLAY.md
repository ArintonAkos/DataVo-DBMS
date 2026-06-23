# Fair Play Benchmark Suite

Objective, in-memory comparison of **DataVo** against market-standard embedded engines (**LiteDB**,
**SQLite** / `sqlite-vec`) across three workloads. Design + fairness rules:
[`../../docs/superpowers/specs/2026-06-23-fair-play-benchmark-suite-design.md`](../../docs/superpowers/specs/2026-06-23-fair-play-benchmark-suite-design.md).

All engines run **in-memory**, with identical data/warm-up and the same measurement harness
(`Stopwatch` + `GC.GetAllocatedBytesForCurrentThread`). The engine interfaces are synchronous so per-op
`Task` allocations don't distort `AllocatedMemory_MB`. No fabricated numbers — an engine that can't run is
reported `n/a`.

## Running

Build once, then run the host with `--format csv`:

```bash
dotnet build demos/Research.Benchmark/src/Research.Benchmark.Host -c Release
DLL=demos/Research.Benchmark/src/Research.Benchmark.Host/bin/Release/net10.0/Research.Benchmark.Host.dll

dotnet "$DLL" --scenario flat-crud      --format csv          # Scenario A (default 50,000 records)
dotnet "$DLL" --scenario deep-document  --format csv --orders 2000   # Scenario B
SQLITE_VEC_PATH=/path/to/vec0.dylib \
dotnet "$DLL" --scenario vector-search  --format csv          # Scenario C (10,000 x 1536-dim, 100 queries)
```

Engine filter: `--engine all|datavo|litedb|sqlite`. Other knobs: `--records`, `--orders`,
`--vectors`, `--dimensions`, `--queries`, `--topk`, `--progress-every`.

## sqlite-vec (Scenario C SQLite column)

`sqlite-vec` is a **native loadable extension**, not a NuGet package. The benchmark loads it from the path
in the `SQLITE_VEC_PATH` environment variable; if it is unset or the library can't load, the SQLite row is
reported `n/a`. To obtain it (example, macOS arm64):

```bash
curl -sL -o /tmp/svec.tgz \
  https://github.com/asg017/sqlite-vec/releases/download/v0.1.6/sqlite-vec-0.1.6-loadable-macos-aarch64.tar.gz
tar xzf /tmp/svec.tgz -C /tmp                      # -> /tmp/vec0.dylib
export SQLITE_VEC_PATH=/tmp/vec0.dylib
```

(Use the matching `loadable-linux-x86_64` asset on Linux CI.)

## Notes on the DataVo results

- **Flat CRUD:** DataVo beats LiteDB (typed in-memory vs BSON); SQLite (native) is fastest. Exposed and
  fixed two real engine bugs (O(n²) keyed-insert validation scan; in-memory index serialized to disk per
  write) — without them DataVo was ~95 s / 119 GB here.
- **Deep Document:** DataVo is materially slower — its compiled-query path only index-accelerates PK/UK, so
  multi-table child reconstruction by FK scans (O(n²)). LiteDB (single BSON doc) and SQLite (indexed FK)
  win. Run at 2,000 orders to keep that O(n²) bounded. Known limitation, not a bug.
- **Vector Search:** DataVo's HNSW gives fast queries (single-digit-ms p99) but a slow build at 1536-dim;
  LiteDB brute force is ~900 ms p99 (no vector index); `sqlite-vec` is fastest end-to-end. HNSW build speed
  is a DataVo optimization target.
