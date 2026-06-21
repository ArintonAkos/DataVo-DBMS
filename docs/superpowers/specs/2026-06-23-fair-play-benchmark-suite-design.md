# "Fair Play" Benchmark Suite — Design

> **Status:** Approved (2026-06-23). Built incrementally A → B → C in `demos/Research.Benchmark`; each
> scenario must emit its results as CSV (in a markdown block) before the next begins.

## Goal

Objectively and *fairly* compare DataVo against market-standard embedded engines (LiteDB, SQLite +
`sqlite-vec`) across three workloads, surfacing where DataVo wins and honestly where it does not.

## Fairness principles (apply to every scenario)

- **All engines in-memory:** DataVo in-memory; LiteDB over a `MemoryStream` (BSON serialize/deserialize is
  still measured — that *is* the comparison point); SQLite `:memory:`. No disk I/O advantage to anyone.
- Identical dataset, identical warm-up, identical iteration counts per engine.
- Each measured phase: `Stopwatch` for wall time + `GC.GetAllocatedBytesForCurrentThread()` for allocation
  (reusing the existing host measurement machinery). Report total execution time, P99 latency, allocated MB.
- Competitor logic implemented correctly and idiomatically (no strawmen) — LiteDB is given its natural
  single-document path in Scenario B; only Scenario C's LiteDB is brute-force *because that is its only
  option* for vector search.
- **No fabricated numbers.** If an engine cannot run in this environment (e.g. native `sqlite-vec` fails to
  load), its row is marked `n/a`, not estimated.

## Architecture (follows the existing benchmark pattern)

- `Research.Benchmark.Abstractions`: one interface + contract record(s) per scenario (mirroring
  `IComplexVipExposureEngine`).
- `Research.Benchmark.Runners`: per-engine runners in new folders `FlatCrud/`, `DeepDocument/`,
  `VectorSearch/`. Add `LiteDB` package; `sqlite-vec` wired via `Microsoft.Data.Sqlite` extension loading
  for Scenario C (attempted).
- `Research.Benchmark.Host/Program.cs`: route `--scenario flat-crud | deep-document | vector-search`; add a
  `--format csv` output mode emitting exactly:
  `Scenario,Engine,ExecutionTime_ms,P99Latency_ms,AllocatedMemory_MB`.
- Tests in `Research.Benchmark.Tests`: a correctness contract test per scenario (each engine returns the
  right data) so the benchmark measures *correct* implementations.

## Scenario A — Flat CRUD (DataVo vs LiteDB)

- **Workload:** 50,000 records (`Id` + a few scalar fields). Phase 1: insert all. Phase 2: point-lookup by
  `Id` ×50,000.
- **DataVo:** typed table; `InsertTyped`; `SELECT … WHERE Id = ?`.
- **LiteDB:** `ILiteCollection<T>` keyed on `Id`; `FindById` per lookup (BSON ser/deser per op).
- **Goal:** pure typed in-memory execution vs BSON overhead.

## Scenario B — Deep Document (DataVo vs LiteDB)

- **Workload:** 5,000 `Order`s, each with ~5 `Item`s and 2 `Address`es. Save all, then load all by id.
- **LiteDB:** one nested BSON document per order — single-read reconstruction (its strength).
- **DataVo:** normalized `Orders` / `OrderItems` / `Addresses` tables; reconstruct each order via multi-table
  JOIN. Acknowledged LiteDB-favorable; included to be honest.

## Scenario C — Vector Search (DataVo vs LiteDB vs SQLite + sqlite-vec)

- **Workload:** insert 10,000 vectors of 1,536-dim `float`; Top-**K=10** nearest-neighbour query ×**100**
  iterations (100 chosen to avoid sandbox timeout on LiteDB brute force while still showing the
  architectural gap).
- **DataVo:** built-in HNSW index.
- **LiteDB:** brute force — load the collection, compute distance in memory, sort, take K.
- **SQLite:** `sqlite-vec` `vec0` virtual table. **Attempted**; if the native extension cannot load in this
  environment, the SQLite row is reported `n/a` (DataVo + LiteDB still shipped).

## Out of scope

- DuckDB / Postgres / Redis for these new scenarios (existing engines stay on their current scenarios).
- Disk-mode variants. Cross-platform native `sqlite-vec` packaging beyond osx-arm64/linux-x64.
