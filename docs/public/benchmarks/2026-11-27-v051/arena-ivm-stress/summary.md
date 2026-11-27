# Arena live-views benchmark

Schema/query revision: `arena-live-views-v1`
Profile: `ivm-stress` (seed 20260721)

| Engine | Architecture | Repetitions | Median total (ms) |
|---|---|---:|---:|
| datavo-reactive | EmbeddedReactiveIvm | 7 | 1799.197 |
| datavo-polling | EmbeddedPollingRecompute | 7 | 11640.690 |
| sqlite | EmbeddedPollingRecompute | 7 | 3312.671 |
| duckdb | EmbeddedPollingRecompute | 7 | 10426.273 |

## datavo-reactive

| Repetition | Total (ms) | Artifact |
|---:|---:|---|
| 1 | 2176.130 | datavo-reactive/repetition-1.json |
| 2 | 1810.868 | datavo-reactive/repetition-2.json |
| 3 | 1799.197 | datavo-reactive/repetition-3.json |
| 4 | 1835.824 | datavo-reactive/repetition-4.json |
| 5 | 1751.989 | datavo-reactive/repetition-5.json |
| 6 | 1761.270 | datavo-reactive/repetition-6.json |
| 7 | 1774.754 | datavo-reactive/repetition-7.json |

## datavo-polling

| Repetition | Total (ms) | Artifact |
|---:|---:|---|
| 1 | 11875.550 | datavo-polling/repetition-1.json |
| 2 | 11733.131 | datavo-polling/repetition-2.json |
| 3 | 11582.130 | datavo-polling/repetition-3.json |
| 4 | 11471.420 | datavo-polling/repetition-4.json |
| 5 | 11640.690 | datavo-polling/repetition-5.json |
| 6 | 11531.704 | datavo-polling/repetition-6.json |
| 7 | 11676.024 | datavo-polling/repetition-7.json |

## sqlite

| Repetition | Total (ms) | Artifact |
|---:|---:|---|
| 1 | 3312.671 | sqlite/repetition-1.json |
| 2 | 3325.689 | sqlite/repetition-2.json |
| 3 | 3349.056 | sqlite/repetition-3.json |
| 4 | 3345.686 | sqlite/repetition-4.json |
| 5 | 3308.928 | sqlite/repetition-5.json |
| 6 | 3300.518 | sqlite/repetition-6.json |
| 7 | 3294.871 | sqlite/repetition-7.json |

## duckdb

| Repetition | Total (ms) | Artifact |
|---:|---:|---|
| 1 | 10852.987 | duckdb/repetition-1.json |
| 2 | 10569.233 | duckdb/repetition-2.json |
| 3 | 10006.743 | duckdb/repetition-3.json |
| 4 | 10899.931 | duckdb/repetition-4.json |
| 5 | 9710.523 | duckdb/repetition-5.json |
| 6 | 9692.484 | duckdb/repetition-6.json |
| 7 | 10426.273 | duckdb/repetition-7.json |
