# CI `concurrent-ops` re-measurement — Run `44916996459` (21 Nov 2026)

- **Workflow run:** [`44916996459`](https://github.com/ArintonAkos/DataVo-DBMS/actions/runs/44916996459) (`master` @ `1b1af5a5`, `DataVo.Core` @ `02fa240f`)
- **Date:** `2026-11-21T15:02:51Z` (Linux) / `2026-11-21T15:03:01Z` (Windows)
- **Workload:** `--scenario concurrent-ops --format markdown` (`100,000` records, `8` readers + `2` writers for `5 s`, `BusyTimeout = 3s`), including both `SQLite` (`shared-cache`) and `SQLite (WAL,normal)`.

## Linux x64 CI (`ubuntu-latest`, Ubuntu 26.04, `linux-x64-net10`)

- `DataVo [DataVo.Core net10.0]`: `7,719,876` reads + `50,986` writes, `1,552,810` OPS
- `SQLite` (`shared-cache`): `1,721,428` reads + `2` writes, `336,952` OPS
- `SQLite (WAL,normal)`: `4,048,881` reads + `49,367` writes, `819,171` OPS

| Engine Name | Total Execution Time (ms) | OPS | Read P99 Latency (ms) | Write P99 Latency (ms) | Total GC Allocated (MB) |
|---|---:|---:|---:|---:|---:|
| DataVo [DataVo.Core net10.0] | 5004.387 | 1552809.809 | 0.005835 | 0.212443 | 1045.075 |
| SQLite | 5108.829 | 336951.951 | 0.094631 | 5105.391351 | 956.031 |
| SQLite (WAL,normal) | 5002.924 | 819170.565 | 0.178383 | 2.515437 | 2259.116 |

## Windows x64 CI (`windows-latest`, Windows 11 24H2 / 10.0.26100, `windows-x64-net10`)

- `DataVo [DataVo.Core net10.0]`: `17,477,611` reads + `0` writes, `1,879,968` OPS (writers starved on 4 logical cores under 8 continuous reader loops; elapsed stretched to `9296.761 ms`)
- `SQLite` (`shared-cache`): `3,855,821` reads + `1` writes, `613,042` OPS
- `SQLite (WAL,normal)`: `2,375,126` reads + `20,592` writes, `339,416` OPS

| Engine Name | Total Execution Time (ms) | OPS | Read P99 Latency (ms) | Write P99 Latency (ms) | Total GC Allocated (MB) |
|---|---:|---:|---:|---:|---:|
| DataVo [DataVo.Core net10.0] | 9296.761 | 1879967.866 | 0.006300 | 0.000000 | 1221.277 |
| SQLite | 6289.653 | 613042.126 | 0.011800 | 6286.278600 | 2175.616 |
| SQLite (WAL,normal) | 7058.352 | 339416.042 | 0.206100 | 3.796100 | 1326.304 |
