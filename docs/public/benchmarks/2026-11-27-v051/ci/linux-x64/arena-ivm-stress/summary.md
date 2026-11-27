# Arena live-views benchmark

Schema/query revision: `arena-live-views-v1`
Profile: `ivm-stress` (seed 20260721)

| Engine | Architecture | Repetitions | Median total (ms) |
|---|---|---:|---:|
| datavo-reactive | EmbeddedReactiveIvm | 7 | 2678.804 |
| datavo-polling | EmbeddedPollingRecompute | 7 | 17084.948 |
| sqlite | EmbeddedPollingRecompute | 7 | 5599.478 |
| duckdb | EmbeddedPollingRecompute | 7 | 30199.517 |

## datavo-reactive

| Repetition | Total (ms) | Artifact |
|---:|---:|---|
| 1 | 3197.165 | datavo-reactive/repetition-1.json |
| 2 | 2744.097 | datavo-reactive/repetition-2.json |
| 3 | 2748.486 | datavo-reactive/repetition-3.json |
| 4 | 2595.290 | datavo-reactive/repetition-4.json |
| 5 | 2678.804 | datavo-reactive/repetition-5.json |
| 6 | 2658.408 | datavo-reactive/repetition-6.json |
| 7 | 2641.969 | datavo-reactive/repetition-7.json |

## datavo-polling

| Repetition | Total (ms) | Artifact |
|---:|---:|---|
| 1 | 17223.566 | datavo-polling/repetition-1.json |
| 2 | 16925.744 | datavo-polling/repetition-2.json |
| 3 | 16885.353 | datavo-polling/repetition-3.json |
| 4 | 17084.948 | datavo-polling/repetition-4.json |
| 5 | 17233.700 | datavo-polling/repetition-5.json |
| 6 | 17124.094 | datavo-polling/repetition-6.json |
| 7 | 17051.123 | datavo-polling/repetition-7.json |

## sqlite

| Repetition | Total (ms) | Artifact |
|---:|---:|---|
| 1 | 5658.417 | sqlite/repetition-1.json |
| 2 | 5614.235 | sqlite/repetition-2.json |
| 3 | 5580.840 | sqlite/repetition-3.json |
| 4 | 5613.778 | sqlite/repetition-4.json |
| 5 | 5581.573 | sqlite/repetition-5.json |
| 6 | 5599.478 | sqlite/repetition-6.json |
| 7 | 5557.087 | sqlite/repetition-7.json |

## duckdb

| Repetition | Total (ms) | Artifact |
|---:|---:|---|
| 1 | 30144.204 | duckdb/repetition-1.json |
| 2 | 30342.131 | duckdb/repetition-2.json |
| 3 | 30130.721 | duckdb/repetition-3.json |
| 4 | 30201.059 | duckdb/repetition-4.json |
| 5 | 30234.961 | duckdb/repetition-5.json |
| 6 | 30121.628 | duckdb/repetition-6.json |
| 7 | 30199.517 | duckdb/repetition-7.json |
