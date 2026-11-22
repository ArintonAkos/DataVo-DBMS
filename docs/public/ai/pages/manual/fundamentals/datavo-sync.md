# DataVo Sync

> Source route: /manual/fundamentals/datavo-sync
> Source file: manual/fundamentals/datavo-sync.md

**DataVo Sync** is a separate, proprietary **commercial add-on** for teams that need offline-first, bidirectional synchronization between embedded DataVo databases and a central **PostgreSQL** server.

> **Not included in `DataVo.Core`:** The open-source MIT-licensed `DataVo.Core` NuGet package and repository contain the standalone embedded database engine only. DataVo Sync is licensed and distributed separately.

## How It Works

DataVo Sync connects PostgreSQL logical replication (`pgoutput`) to embedded DataVo clients over WebSocket and HTTP, keeping local reads microsecond-fast while synchronizing mutations automatically when online:

- **Wire v2 + Brotli framing:** Compact binary delta frames averaging **45.5 B/row** over the wire.
- **Z-set delta compaction:** Coalesces redundant intermediate mutations before transmission so offline bursts converge cleanly.
- **Per-column conflict resolution:** Resolves concurrent offline edits at column granularity rather than overwriting whole rows.
- **Crash-recoverable write-back:** Local offline writes are persisted and replayed safely to PostgreSQL upon reconnection.

## Verified Benchmark Highlights

Measured in end-to-end Linux x64 CI benchmarks (20 November 2026, PostgreSQL 17.12, 2 vCPU VM):

| Metric | DataVo Sync | Comparison Baseline |
| --- | --- | --- |
| **100k warm initial sync** | **605 ms** (`InMemory`) · **706–726 ms** (`Lsm`) | 702–909 ms (SQLite/JSON) · 1,167 ms (ElectricSQL `1.8.1`) |
| **Wire footprint** | **45.5 B/row** (wire v2 + Brotli) | 362.7 B/row ElectricSQL (**8.0× smaller**) |
| **Client allocations (100k sync)** | **36.4–49.1 MB** | 251.4 MB ElectricSQL (**5.1–6.9× lower**) |
| **Concurrent scaling** | **1,000 WebSocket clients** (`12,754 rows/s`) | Single 2-vCPU VM |
| **Local read & convergence** | **4.06–5.19 µs** point lookups · **81–243 ms** offline-burst convergence | Across PostgreSQL and peers |

## When to Use DataVo Sync

- **Local-first .NET desktop, mobile, or edge apps** that must stay fully responsive offline and sync with an existing PostgreSQL backend.
- **Multi-client collaborative tools** that combine local SQL/LINQ queries and [Live Reactive Views](/manual/fundamentals/reactive-views) with shared server state.
- **High-density client fleets** where wire bandwidth, client GC overhead, and initial hydration time directly affect hosting cost.

## Licensing & Early Access

For commercial licensing, evaluation builds, or architecture inquiries, reach out to `@ArintonAkos` on GitHub.

## See Also

- [LSM & Durability](/manual/fundamentals/lsm-and-durability) — Local crash-safe persistence used by offline DataVo clients
- [Live Reactive Views](/manual/fundamentals/reactive-views) — Real-time UI updates driven by local and synced changes
- [Benchmarks](/manual/performance/benchmarks) — Embedded engine performance benchmarks
