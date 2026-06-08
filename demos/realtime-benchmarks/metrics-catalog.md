# Metrics Catalog

The demos should produce numbers that can be defended. These are the metrics we report and why they
matter.

## Latency

- `tickLatency`: total per-loop cost, including mutations and live-view maintenance.
- `mutationLatency`: time spent applying the base-table writes.
- `viewMaintenanceLatency`: time spent updating or recomputing live views.
- percentiles: p50, p90, p95, p99, p99.9, max.

Why p99 matters:

- games care about missed frames, not only average frame time
- trading dashboards and operations consoles care about tail delay under bursts
- GC pauses and occasional full recomputes show up in tail latency

## Frame Budget

- 60Hz budget: `16.666ms`
- 120Hz budget: `8.333ms`

Reported:

- `frameBudgetMissRate60Hz`
- `frameBudgetMissRate120Hz`

## Work Avoided

- reactive: `added + removed + updated`
- polling: total rows returned by all live-view queries

Interpretation:

```text
work avoided ~= pollingRowsReturned / max(1, reactiveDeltaRows)
```

This is not a universal speedup ratio. It is a signal of how much result traffic the app avoids by
receiving deltas instead of full snapshots.

## Memory And GC

- `allocatedBytes`: bytes allocated during the measured run.
- `liveMemoryDeltaBytes`: approximate heap growth.
- `gen0/gen1/gen2 collections`: collection counts during the measured run.
- `pauseP99Ms` and `pauseMaxMs`: GC pause durations when exposed by the runtime.

Why it matters:

- low-latency systems often fail in the tail because of allocation and GC, not the median path
- game loops and HFT-style dashboards need predictable p99 behavior

## Recommended Reporting Table

| Scenario | Architecture | Rows | Mutations/tick | p50 view ms | p99 view ms | Delta rows | Poll rows | Alloc MB | Gen2 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Game Arena | DataVo reactive | 100k | 500 | measured | measured | measured | 0 | measured | measured |
| Game Arena | Polling | 100k | 500 | measured | measured | 0 | measured | measured | measured |

## Rules For Honest Results

- Always show hardware, OS, .NET version, DataVo commit, storage mode, rows, ticks, warmup, and seed.
- Report both reactive and polling runs.
- Do not compare browser timings directly with native timings.
- Do not claim exchange-core HFT suitability from the trading demo; it models live read views.
