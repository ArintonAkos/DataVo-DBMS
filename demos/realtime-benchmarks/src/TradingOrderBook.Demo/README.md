# TradingOrderBook.Demo

This benchmark models the live read side of a trading/risk system: order-book extrema, account
exposure, and recent trades. It is not a matching-engine benchmark and does not claim exchange-core
HFT suitability. It measures realtime dashboard/risk view maintenance under order churn.

## Architectures Compared

- **DataVo reactive subscriptions:** SQL views are incrementally maintained from committed deltas.
- **Polling full recompute:** each tick reruns all dashboard SQL queries, representing a common
  embedded database pattern and a simplified SQLite polling architecture.

## Workload

- `Orders(OrderId, Symbol, Side, Price, Qty, Status)`
- `Trades(TradeId, Symbol, Price, Qty, Ts)`
- `Positions(PositionId, AccountId, Symbol, Qty)`
- Per tick: update order price/quantity/status, insert trades, update positions.
- Live views:
  - best bid per symbol via `MAX(Price)`
  - best ask per symbol via `MIN(Price)`
  - net position per account/symbol via `SUM(Qty)`
  - recent trades via `ORDER BY Ts DESC LIMIT 100`

## Metrics

The JSON output includes:

- p50/p90/p95/p99/p99.9/max tick latency
- mutation latency and view-maintenance latency
- delta rows emitted by DataVo versus rows returned by polling
- total allocated bytes and GC collection counts
- GC pause p99/max when exposed by the runtime

## Run

Small smoke run:

```bash
dotnet run --project demos/realtime-benchmarks/src/TradingOrderBook.Demo/TradingOrderBook.Demo.csproj -- --rows 2000 --ticks 50 --warmup 5 --mutations 25
```

Larger local run:

```bash
dotnet run --project demos/realtime-benchmarks/src/TradingOrderBook.Demo/TradingOrderBook.Demo.csproj -- --rows 100000 --ticks 5000 --warmup 250 --mutations 500 --out artifacts/benchmarks/trading-order-book.json
```

## Why DataVo Can Compete Here

The benchmark targets live read models, where the application cares about what changed since the
last tick rather than a full snapshot. DataVo maintains aggregates and top-k views incrementally,
including `MIN`/`MAX` using duplicate-aware extremum state.

Relevant research:

- Budiu, McSherry, Ryzhyk, Tannen — **DBSP**. Incremental query maintenance from input deltas.
- Behm et al. — **Photon** and Pedreira et al. — **Velox**. These motivate measuring allocation and
  GC pressure for low-latency data systems.
- Thompson et al. — **Disruptor**. Relevant future work for lock-free ingestion, not implemented in
  this benchmark today.

## Pros / Cons

Pros:

- direct SQL definition for trading dashboard views
- no external streaming stack required for embedded read-model maintenance
- p99 view-maintenance latency is measured separately from write cost

Cons:

- DataVo is currently a preview embedded engine, not a replacement for dedicated exchange-core
  infrastructure
- the benchmark uses SQL DML for mutations, so it measures the real current stack rather than an
  idealized low-level ingestion path
- multi-subscription state sharing is future work; each subscription currently owns its operator state
