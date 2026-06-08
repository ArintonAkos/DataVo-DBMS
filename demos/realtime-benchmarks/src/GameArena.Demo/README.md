# GameArena.Demo

This benchmark models a realtime multiplayer game server tick loop. The game mutates base tables
for player position, health, score, and inventory, then refreshes views used by clients and HUDs.

## Architectures Compared

- **DataVo reactive subscriptions:** standing SQL queries are seeded once and maintained from
  committed row deltas. Each tick calls `DispatchPendingNotifications()` on the game loop thread.
- **Polling full recompute:** the same live-view SQL is rerun every tick, which is the common
  SQLite-style embedded architecture when applications need realtime UI/network state.

This is intentionally not a blanket "DataVo beats SQLite" benchmark. It measures the specific
case where applications repeatedly need live query results after small mutations.

## Workload

- `Players(Id, Zone, Team, X, Y, Health, Score)`
- `Inventory(PlayerId, ItemId, Slot)`
- Per tick: update random player positions, health, score, and zone.
- Live views:
  - players visible in one arena zone
  - alive players per team
  - top-20 leaderboard
  - player/inventory join for that zone

## Metrics

The JSON output includes:

- tick latency p50/p90/p95/p99/p99.9/max
- mutation latency and view-maintenance latency
- 60Hz and 120Hz frame-budget miss rates
- DataVo delta rows emitted versus polling rows returned
- total allocated bytes, live memory delta, GC collection counts, and GC pause summary

## Run

Small smoke run:

```bash
dotnet run --project demos/realtime-benchmarks/src/GameArena.Demo/GameArena.Demo.csproj -- --rows 1000 --ticks 50 --warmup 5 --mutations 25
```

Larger local run:

```bash
dotnet run --project demos/realtime-benchmarks/src/GameArena.Demo/GameArena.Demo.csproj -- --rows 100000 --ticks 5000 --warmup 250 --mutations 500 --out artifacts/benchmarks/game-arena.json
```

## Why DataVo Can Compete Here

DataVo's reactive layer implements incremental view maintenance over SQL operators. Instead of
asking the application to maintain bespoke caches or rerun full queries every frame, DataVo pushes
committed row deltas through maintained query operators.

Relevant research already reflected in the reactive implementation:

- Budiu, McSherry, Ryzhyk, Tannen — **DBSP: Automatic Incremental View Maintenance for Rich Query Languages**.
  This is the core model behind maintaining query results from deltas.
- Gjengset et al. — **Noria**. A production-oriented precedent for incrementally maintained backend views.
- Orsten — **Dynamically Learning Efficient Server/Client Network Protocols for Networked Simulations**.
  Relevant to game-state snapshot/delta delivery.

## Pros / Cons

Pros:

- deterministic game-loop delivery; no background reactive thread
- fewer rows emitted to the application when only small parts of the view change
- one SQL definition for persistence and live views

Cons:

- reactive operators hold state, so memory usage scales with subscribed view shapes
- unsupported SQL shapes must still fall back to polling or be added to the reactive engine
- this benchmark uses DataVo's SQL pipeline for writes, so raw hand-coded state mutation can still be faster
