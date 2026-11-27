# Arena live-views benchmark

Schema/query revision: `arena-live-views-v1`
Profile: `gameplay` (seed 20260721)

| Engine | Architecture | Repetitions | Median total (ms) |
|---|---|---:|---:|
| datavo-reactive | EmbeddedReactiveIvm | 7 | 368.800 |
| datavo-polling | EmbeddedPollingRecompute | 7 | 487.937 |
| sqlite | EmbeddedPollingRecompute | 7 | 214.817 |
| duckdb | EmbeddedPollingRecompute | 7 | 2627.489 |

## datavo-reactive

| Repetition | Total (ms) | Artifact |
|---:|---:|---|
| 1 | 368.800 | datavo-reactive/repetition-1.json |
| 2 | 396.793 | datavo-reactive/repetition-2.json |
| 3 | 439.075 | datavo-reactive/repetition-3.json |
| 4 | 410.241 | datavo-reactive/repetition-4.json |
| 5 | 317.975 | datavo-reactive/repetition-5.json |
| 6 | 231.227 | datavo-reactive/repetition-6.json |
| 7 | 224.148 | datavo-reactive/repetition-7.json |

## datavo-polling

| Repetition | Total (ms) | Artifact |
|---:|---:|---|
| 1 | 1628.979 | datavo-polling/repetition-1.json |
| 2 | 1352.211 | datavo-polling/repetition-2.json |
| 3 | 539.946 | datavo-polling/repetition-3.json |
| 4 | 487.937 | datavo-polling/repetition-4.json |
| 5 | 462.566 | datavo-polling/repetition-5.json |
| 6 | 410.835 | datavo-polling/repetition-6.json |
| 7 | 437.628 | datavo-polling/repetition-7.json |

## sqlite

| Repetition | Total (ms) | Artifact |
|---:|---:|---|
| 1 | 294.376 | sqlite/repetition-1.json |
| 2 | 218.386 | sqlite/repetition-2.json |
| 3 | 226.018 | sqlite/repetition-3.json |
| 4 | 203.561 | sqlite/repetition-4.json |
| 5 | 199.665 | sqlite/repetition-5.json |
| 6 | 214.817 | sqlite/repetition-6.json |
| 7 | 201.404 | sqlite/repetition-7.json |

## duckdb

| Repetition | Total (ms) | Artifact |
|---:|---:|---|
| 1 | 2790.297 | duckdb/repetition-1.json |
| 2 | 2589.931 | duckdb/repetition-2.json |
| 3 | 2638.444 | duckdb/repetition-3.json |
| 4 | 2628.196 | duckdb/repetition-4.json |
| 5 | 2600.283 | duckdb/repetition-5.json |
| 6 | 2538.949 | duckdb/repetition-6.json |
| 7 | 2627.489 | duckdb/repetition-7.json |
