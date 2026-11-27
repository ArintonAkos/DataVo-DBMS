# ycsb-mixed variance check (3 extra serial runs, same machine, 2026-11-27, after the main pass)

## Rerun 1
| Engine Name | Total Execution Time (ms) | OPS | Read P99 Latency (ms) | Write P99 Latency (ms) | Total GC Allocated (MB) |
|---|---:|---:|---:|---:|---:|
| DataVo (LSM Production) [DataVo.Core net10.0] | 23160.764 | 4317.647 | 0.022500 | 33.667292 | 37.213 |
| DataVo (LSM Relaxed) [DataVo.Core net10.0] | 94.979 | 1052867.643 | 0.000792 | 1.253792 | 34.735 |
| SQLite (WAL,normal) | 421.310 | 237355.198 | 0.009792 | 3.407792 | 57.959 |
| LiteDB | 2800.720 | 35705.098 | 2.383417 | 2.658625 | 4318.926 |

## Rerun 2
| Engine Name | Total Execution Time (ms) | OPS | Read P99 Latency (ms) | Write P99 Latency (ms) | Total GC Allocated (MB) |
|---|---:|---:|---:|---:|---:|
| DataVo (LSM Production) [DataVo.Core net10.0] | 22245.433 | 4495.305 | 0.021125 | 32.285750 | 37.235 |
| DataVo (LSM Relaxed) [DataVo.Core net10.0] | 549.308 | 182047.065 | 0.001083 | 0.770916 | 34.735 |
| SQLite (WAL,normal) | 436.423 | 229135.757 | 0.010583 | 3.414042 | 57.812 |
| LiteDB | 2800.325 | 35710.146 | 2.346125 | 2.557042 | 4318.849 |

## Rerun 3
| Engine Name | Total Execution Time (ms) | OPS | Read P99 Latency (ms) | Write P99 Latency (ms) | Total GC Allocated (MB) |
|---|---:|---:|---:|---:|---:|
| DataVo (LSM Production) [DataVo.Core net10.0] | 21602.953 | 4628.997 | 0.022667 | 31.349125 | 37.173 |
| DataVo (LSM Relaxed) [DataVo.Core net10.0] | 486.903 | 205379.674 | 0.001000 | 0.895375 | 34.735 |
| SQLite (WAL,normal) | 417.334 | 239616.231 | 0.010209 | 3.412541 | 56.961 |
| LiteDB | 2806.576 | 35630.603 | 2.792708 | 3.198416 | 4312.546 |
