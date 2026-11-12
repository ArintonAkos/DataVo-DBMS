| Engine Name | Total Execution Time (ms) | OPS | Read P99 Latency (ms) | Write P99 Latency (ms) | Total GC Allocated (MB) |
|---|---:|---:|---:|---:|---:|
| DataVo (LSM Production) [DataVo.Core net10.0] | 23118.636 | 4325.515 | 0.025708 | 27.831250 | 37.233 |
| DataVo (LSM Relaxed) [DataVo.Core net10.0] | 195.812 | 510694.713 | 0.000875 | 0.656708 | 34.734 |
| SQLite (WAL,normal) | 382.237 | 261617.997 | 0.008167 | 3.771417 | 57.247 |
| LiteDB | 2640.107 | 37877.257 | 2.589708 | 2.788291 | 4315.119 |
