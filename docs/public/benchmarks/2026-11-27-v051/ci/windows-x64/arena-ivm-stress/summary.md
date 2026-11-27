# Arena live-views benchmark

Schema/query revision: `arena-live-views-v1`
Profile: `ivm-stress` (seed 20260721)

| Engine | Architecture | Repetitions | Median total (ms) |
|---|---|---:|---:|
| datavo-reactive | EmbeddedReactiveIvm | 7 | 3598.395 |
| datavo-polling | EmbeddedPollingRecompute | 7 | 22071.871 |
| sqlite | EmbeddedPollingRecompute | 7 | 7268.418 |
| duckdb | EmbeddedPollingRecompute | 7 | 46556.309 |

## datavo-reactive

| Repetition | Total (ms) | Artifact |
|---:|---:|---|
| 1 | 4365.101 | datavo-reactive/repetition-1.json |
| 2 | 3522.791 | datavo-reactive/repetition-2.json |
| 3 | 3624.687 | datavo-reactive/repetition-3.json |
| 4 | 3606.512 | datavo-reactive/repetition-4.json |
| 5 | 3574.663 | datavo-reactive/repetition-5.json |
| 6 | 3530.243 | datavo-reactive/repetition-6.json |
| 7 | 3598.395 | datavo-reactive/repetition-7.json |

## datavo-polling

| Repetition | Total (ms) | Artifact |
|---:|---:|---|
| 1 | 21497.798 | datavo-polling/repetition-1.json |
| 2 | 21464.807 | datavo-polling/repetition-2.json |
| 3 | 22071.871 | datavo-polling/repetition-3.json |
| 4 | 23391.001 | datavo-polling/repetition-4.json |
| 5 | 24615.599 | datavo-polling/repetition-5.json |
| 6 | 23641.477 | datavo-polling/repetition-6.json |
| 7 | 21844.410 | datavo-polling/repetition-7.json |

## sqlite

| Repetition | Total (ms) | Artifact |
|---:|---:|---|
| 1 | 7439.988 | sqlite/repetition-1.json |
| 2 | 7392.322 | sqlite/repetition-2.json |
| 3 | 7233.376 | sqlite/repetition-3.json |
| 4 | 7268.418 | sqlite/repetition-4.json |
| 5 | 7219.140 | sqlite/repetition-5.json |
| 6 | 7255.477 | sqlite/repetition-6.json |
| 7 | 7307.423 | sqlite/repetition-7.json |

## duckdb

| Repetition | Total (ms) | Artifact |
|---:|---:|---|
| 1 | 46556.309 | duckdb/repetition-1.json |
| 2 | 46585.097 | duckdb/repetition-2.json |
| 3 | 46742.561 | duckdb/repetition-3.json |
| 4 | 46412.234 | duckdb/repetition-4.json |
| 5 | 46941.101 | duckdb/repetition-5.json |
| 6 | 46501.497 | duckdb/repetition-6.json |
| 7 | 46555.959 | duckdb/repetition-7.json |
