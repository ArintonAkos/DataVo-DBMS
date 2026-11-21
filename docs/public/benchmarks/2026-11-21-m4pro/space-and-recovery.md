# Apple M4 Pro `space-and-recovery` (21 Nov 2026)

- **Commit:** `02fa240f`
- **Machine:** Apple M4 Pro (macOS arm64, .NET 10.0)
- **Command:** `dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host -- --scenario space-and-recovery --records 1000000 --format markdown`

## Cold-open first read latency

- `DataVo (LSM Production) [DataVo.Core net10.0]`: insert `704.9 ms` | size `27.72 MB` | recovery `4.5 ms` | first read after recovery `3.8 ms`
- `DataVo (LSM Relaxed) [DataVo.Core net10.0]`: insert `403.1 ms` | size `27.72 MB` | recovery `1.7 ms` | first read after recovery `0.0 ms`
- `SQLite (WAL,normal)`: insert `697.4 ms` | size `29.08 MB` | recovery `0.4 ms` | first read after recovery `1.2 ms`
- `LiteDB`: insert `2,980.4 ms` | size `154.46 MB` | recovery `0.6 ms` | first read after recovery `9.6 ms`

## Summary table

| Engine Name | Insert Execution Time (ms) | Disk Size (MB) | Recovery Time (ms) | Total GC Allocated (MB) |
|---|---:|---:|---:|---:|
| DataVo (LSM Production) [DataVo.Core net10.0] | 704.854 | 27.725 | 4.454 | 411.659 |
| DataVo (LSM Relaxed) [DataVo.Core net10.0] | 403.059 | 27.725 | 1.717 | 345.962 |
| SQLite (WAL,normal) | 697.350 | 29.082 | 0.373 | 632.935 |
| LiteDB | 2980.369 | 154.461 | 0.637 | 15107.177 |
