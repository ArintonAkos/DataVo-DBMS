# Arena live-views benchmark

Schema/query revision: `arena-live-views-v1`
Profile: `gameplay` (seed 20260721)

| Engine | Architecture | Repetitions | Median total (ms) |
|---|---|---:|---:|
| datavo-reactive | EmbeddedReactiveIvm | 7 | 227.562 |
| datavo-polling | EmbeddedPollingRecompute | 7 | 373.374 |
| sqlite | EmbeddedPollingRecompute | 7 | 141.459 |
| duckdb | EmbeddedPollingRecompute | 7 | 1733.648 |

## datavo-reactive

| Repetition | Total (ms) | Artifact |
|---:|---:|---|
| 1 | 212.000 | datavo-reactive/repetition-1.json |
| 2 | 245.227 | datavo-reactive/repetition-2.json |
| 3 | 227.814 | datavo-reactive/repetition-3.json |
| 4 | 248.182 | datavo-reactive/repetition-4.json |
| 5 | 227.562 | datavo-reactive/repetition-5.json |
| 6 | 175.780 | datavo-reactive/repetition-6.json |
| 7 | 142.841 | datavo-reactive/repetition-7.json |

## datavo-polling

| Repetition | Total (ms) | Artifact |
|---:|---:|---|
| 1 | 932.819 | datavo-polling/repetition-1.json |
| 2 | 944.710 | datavo-polling/repetition-2.json |
| 3 | 775.447 | datavo-polling/repetition-3.json |
| 4 | 373.374 | datavo-polling/repetition-4.json |
| 5 | 316.822 | datavo-polling/repetition-5.json |
| 6 | 312.613 | datavo-polling/repetition-6.json |
| 7 | 314.927 | datavo-polling/repetition-7.json |

## sqlite

| Repetition | Total (ms) | Artifact |
|---:|---:|---|
| 1 | 168.777 | sqlite/repetition-1.json |
| 2 | 146.178 | sqlite/repetition-2.json |
| 3 | 141.942 | sqlite/repetition-3.json |
| 4 | 141.459 | sqlite/repetition-4.json |
| 5 | 141.203 | sqlite/repetition-5.json |
| 6 | 141.180 | sqlite/repetition-6.json |
| 7 | 140.433 | sqlite/repetition-7.json |

## duckdb

| Repetition | Total (ms) | Artifact |
|---:|---:|---|
| 1 | 1742.909 | duckdb/repetition-1.json |
| 2 | 1741.043 | duckdb/repetition-2.json |
| 3 | 1726.989 | duckdb/repetition-3.json |
| 4 | 1739.087 | duckdb/repetition-4.json |
| 5 | 1732.759 | duckdb/repetition-5.json |
| 6 | 1721.428 | duckdb/repetition-6.json |
| 7 | 1733.648 | duckdb/repetition-7.json |
