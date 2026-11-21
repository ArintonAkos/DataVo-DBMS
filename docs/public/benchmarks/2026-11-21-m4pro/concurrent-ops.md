# Apple M4 Pro `concurrent-ops` (21 Nov 2026)

- **Commit:** `02fa240f` (with `SqliteConcurrentOpsEngine` parameterized for `SQLite (WAL,normal)` comparator)
- **Machine:** Apple M4 Pro (macOS arm64, .NET 10.0)
- **Command:** `dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host -- --scenario concurrent-ops --format markdown`
- **Workload:** 100,000 preloaded records, 8 reader threads + 2 writer threads for 5 s (`BusyTimeout = 3s`)

## Per-engine read / write counts

- `DataVo [DataVo.Core net10.0]`: `5,214,990` reads + `224,522` writes, `1,087,329` OPS
- `SQLite` (`mode=memory&cache=shared`): `1,914,864` reads + `2` writes, `381,982` OPS (table-level shared-cache locking lets 8 continuous readers starve writers into repeated `busy_timeout = 3000` expirations)
- `SQLite (WAL,normal)` (temp file, `journal_mode=WAL`, `synchronous=NORMAL`): `1,857,503` reads + `91,458` writes, `389,341` OPS

## Summary table

| Engine Name | Total Execution Time (ms) | OPS | Read P99 Latency (ms) | Write P99 Latency (ms) | Total GC Allocated (MB) |
|---|---:|---:|---:|---:|---:|
| DataVo [DataVo.Core net10.0] | 5002.638 | 1087328.660 | 0.189708 | 0.619834 | 2460.431 |
| SQLite | 5012.975 | 381981.980 | 0.110959 | 5011.840208 | 1054.680 |
| SQLite (WAL,normal) | 5005.792 | 389341.211 | 0.142208 | 1.299083 | 1075.223 |

> Note: An earlier unmodified M4 Pro run (`DataVo` + shared-cache `SQLite` only) on the same day measured `DataVo` at `1,468,680.543` OPS (`7,155,044` reads + `192,193` writes, Read P99 `0.011208 ms`, Write P99 `0.880667 ms`, GC `2322.421 MB`) and shared-cache `SQLite` at `381,454.793` OPS (`1,911,933` reads + `2` writes, Read P99 `0.114083 ms`, Write P99 `5011.326375 ms`, GC `1052.569 MB`). All three rows above come from a single back-to-back run.
