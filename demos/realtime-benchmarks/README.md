# DataVo Realtime Benchmarks

This suite contains runnable demos for DataVo's strongest current differentiator:

> embedded SQL plus incremental live-query deltas, measured against full-query polling baselines.

The goal is not to claim DataVo is universally faster than SQLite, Kafka, Flink, Materialize, or a
hand-written cache. The honest claim being measured is narrower and stronger: when an application
needs live views after frequent small mutations, DataVo can maintain SQL result deltas directly in
process instead of making the application repeatedly poll and recompute whole views.

## Projects

- `src/DataVo.Benchmarks.Common` — shared latency, GC, result, and DataVo execution helpers.
- `src/GameArena.Demo` — game-server tick-loop benchmark for live player state, aggregates, joins,
  and leaderboards.
- `src/TradingOrderBook.Demo` — trading/risk read-model benchmark for top-of-book, net positions,
  and recent trades.
- `src/LiveDeltaGateway.Demo` — ASP.NET Core + browser UI where the client sends SQL subscriptions
  over WebSocket and receives DataVo `QueryChange` deltas.
- `browser-wasm-showcase` — compatibility notes and next-step design for running the same idea in
  the existing `DataVo.Browser` WebAssembly runtime.
- `architecture-comparison.md` — diagrams for SQLite polling, app-maintained caches, external
  streaming stacks, and DataVo reactive.
- `metrics-catalog.md` — p50/p95/p99, GC, frame-budget, and work-avoided definitions.

## Metrics

Each runnable demo emits JSON with:

- tick latency p50/p90/p95/p99/p99.9/max
- mutation latency
- view-maintenance latency
- 60Hz and 120Hz frame-budget miss rates
- DataVo delta rows emitted or polling rows returned
- total allocated bytes and live memory delta
- Gen0/Gen1/Gen2 collection counts
- GC pause summary when exposed by the runtime

## Quick Start

Game arena smoke run:

```bash
dotnet run --project demos/realtime-benchmarks/src/GameArena.Demo/GameArena.Demo.csproj -- --rows 1000 --ticks 50 --warmup 5 --mutations 25
```

Trading smoke run:

```bash
dotnet run --project demos/realtime-benchmarks/src/TradingOrderBook.Demo/TradingOrderBook.Demo.csproj -- --rows 2000 --ticks 50 --warmup 5 --mutations 25
```

Live WebSocket UI:

```bash
dotnet run --project demos/realtime-benchmarks/src/LiveDeltaGateway.Demo/LiveDeltaGateway.Demo.csproj
```

Then open the URL printed by ASP.NET Core, usually:

```text
http://localhost:5000
```

Run only DataVo reactive architecture:

```bash
dotnet run --project demos/realtime-benchmarks/src/GameArena.Demo/GameArena.Demo.csproj -- --mode reactive
```

Run only polling baseline:

```bash
dotnet run --project demos/realtime-benchmarks/src/GameArena.Demo/GameArena.Demo.csproj -- --mode polling
```

Disk-backed run:

```bash
dotnet run --project demos/realtime-benchmarks/src/TradingOrderBook.Demo/TradingOrderBook.Demo.csproj -- --storage disk
```

## Compared Architectures

### DataVo Reactive

DataVo registers standing SQL queries with `Subscribe(sql, callback)`, buffers committed base-table
changes, and applies them through incremental operators on `DispatchPendingNotifications()`.

This models local-first apps, game loops, and embedded dashboards where the process owns both writes
and live views.

### Polling Full Recompute

The baseline reruns each live-view SQL query after every mutation batch. This is the usual embedded
database architecture when the database does not maintain query result deltas for the application.

This is the fair SQLite-style comparison: SQLite is excellent embedded storage, but the application
usually owns live-view invalidation and recomputation.

### External Streaming Stack

Kafka/Flink/Materialize-style architectures can be the right answer for distributed systems. The
tradeoff is operational complexity: brokers, connectors, stream processors, schemas, and network
serialization. DataVo's comparison point is the embedded/local case where an in-process incremental
view engine can remove that stack.

## Research Lineage

Implemented or directly reflected in DataVo today:

- Budiu, McSherry, Ryzhyk, Tannen — **DBSP: Automatic Incremental View Maintenance for Rich Query
  Languages**. DataVo's reactive work follows the same delta-through-operators framing.
- Gjengset et al. — **Noria**. Production precedent for incrementally maintained backend views.

Important motivation/future work:

- McSherry et al. — **Shared Arrangements**. Relevant to future state sharing across many
  subscriptions; current DataVo subscriptions own their operator state.
- Behm et al. — **Photon** and Pedreira et al. — **Velox**. Motivate measuring allocation and GC in
  low-latency data systems.
- Thompson et al. — **Disruptor**. Relevant to future lock-free event ingestion; not implemented in
  these demos.
- Orsten — **Dynamically Learning Efficient Server/Client Network Protocols for Networked
  Simulations**. Relevant to game-state delta delivery.

## Interpreting Results

Look for:

- lower p95/p99 view-maintenance latency for reactive mode when mutation batches are small relative
  to total table size
- lower rows emitted by reactive mode than rows returned by polling
- frame-budget miss rates under 60Hz or 120Hz targets
- allocation and GC differences between the two architectures

Expect polling to look competitive on tiny datasets and simple queries. DataVo's advantage should
become clearer as table size and view complexity grow while per-tick mutations remain bounded.
