# Arena live-views benchmark

Schema/query revision: `arena-live-views-v1`
Profile: `gameplay` (seed 20260721)

| Engine | Architecture | Repetitions | Median total (ms) |
|---|---|---:|---:|
| datavo-reactive | EmbeddedReactiveIvm | 7 | 107.777 |
| datavo-polling | EmbeddedPollingRecompute | 7 | 424.968 |
| sqlite | EmbeddedPollingRecompute | 7 | 89.685 |
| duckdb | EmbeddedPollingRecompute | 7 | 563.038 |

## datavo-reactive

| Repetition | Total (ms) | Artifact |
|---:|---:|---|
| 1 | 90.851 | datavo-reactive/repetition-1.json |
| 2 | 97.951 | datavo-reactive/repetition-2.json |
| 3 | 99.593 | datavo-reactive/repetition-3.json |
| 4 | 110.546 | datavo-reactive/repetition-4.json |
| 5 | 120.141 | datavo-reactive/repetition-5.json |
| 6 | 108.267 | datavo-reactive/repetition-6.json |
| 7 | 107.777 | datavo-reactive/repetition-7.json |

## datavo-polling

| Repetition | Total (ms) | Artifact |
|---:|---:|---|
| 1 | 576.460 | datavo-polling/repetition-1.json |
| 2 | 442.245 | datavo-polling/repetition-2.json |
| 3 | 424.968 | datavo-polling/repetition-3.json |
| 4 | 426.606 | datavo-polling/repetition-4.json |
| 5 | 388.153 | datavo-polling/repetition-5.json |
| 6 | 186.798 | datavo-polling/repetition-6.json |
| 7 | 161.635 | datavo-polling/repetition-7.json |

## sqlite

| Repetition | Total (ms) | Artifact |
|---:|---:|---|
| 1 | 90.311 | sqlite/repetition-1.json |
| 2 | 89.685 | sqlite/repetition-2.json |
| 3 | 91.419 | sqlite/repetition-3.json |
| 4 | 90.397 | sqlite/repetition-4.json |
| 5 | 80.067 | sqlite/repetition-5.json |
| 6 | 80.850 | sqlite/repetition-6.json |
| 7 | 78.633 | sqlite/repetition-7.json |

## duckdb

| Repetition | Total (ms) | Artifact |
|---:|---:|---|
| 1 | 609.054 | duckdb/repetition-1.json |
| 2 | 514.726 | duckdb/repetition-2.json |
| 3 | 516.250 | duckdb/repetition-3.json |
| 4 | 522.677 | duckdb/repetition-4.json |
| 5 | 563.038 | duckdb/repetition-5.json |
| 6 | 595.884 | duckdb/repetition-6.json |
| 7 | 626.038 | duckdb/repetition-7.json |
