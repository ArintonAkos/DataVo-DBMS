| Engine Name | Total Execution Time (ms) | OPS | Read P99 Latency (ms) | Write P99 Latency (ms) | Total GC Allocated (MB) |
|---|---:|---:|---:|---:|---:|
| DataVo (LSM Production) [DataVo.Core net10.0] | 25089.933 | 3985.662 | 0.027625 | 36.063167 | 37.241 |
| DataVo (LSM Relaxed) [DataVo.Core net10.0] | 288.296 | 346865.601 | 0.001000 | 1.055708 | 34.735 |
| SQLite (WAL,normal) | 441.934 | 226277.978 | 0.009375 | 3.402666 | 56.985 |
| LiteDB | 2730.470 | 36623.727 | 2.683958 | 2.963625 | 4314.263 |
