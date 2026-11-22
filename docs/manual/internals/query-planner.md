---
title: Query Planner & Fast Paths
description: How DataVo routes SELECT queries through typed single-table, typed join, and hybrid vector fast paths before falling back to the general evaluator.
---

# Query Planner & Fast Paths

DataVo does not use a cost-based optimizer. Instead, it pairs one general evaluator that can execute every supported SQL query with a cascade of **typed fast paths** that operate directly on stored bytes without boxing. Whether a query stays on a typed path is the single largest factor in its latency and allocation profile.

```csharp
using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = "./app_db" });

// Stays on the Typed Single-Table Path (index lookup or UTF-8 byte scan, ~16–24 KB alloc / 1M rows)
var stmt = db.Prepare("SELECT Id, Name, Score FROM players WHERE Region = @r AND Score >= @min");
var result = stmt.Execute(new DataVoQueryParameter("r", "EU"), new DataVoQueryParameter("min", 500));
```

## Dispatch Cascade

Every `SELECT` is offered to the fast paths in order. A fast path either returns the exact same rows as the general evaluator—in the same order—or declines cleanly; declining is never an error.

<div class="sketch-diagram">
  <img class="light-only" src="/manual/fig-05-planner-fast-paths-light.svg" alt="Query planner dispatch cascade trying Typed Single-Table, Typed Join, and Hybrid Vector paths before the General Evaluator fallback" />
  <img class="dark-only" src="/manual/fig-05-planner-fast-paths-dark.svg" alt="Query planner dispatch cascade trying Typed Single-Table, Typed Join, and Hybrid Vector paths before the General Evaluator fallback" />
</div>

The general evaluator materializes every scanned row as a `Dictionary<string, object?>` (2–4 KB per row read), which turns a 1,000,000-row scan into gigabytes of GC pressure and multi-second latency. Moving `OR`, `LIKE`, and multi-table joins onto typed paths reduced those queries from **1.7–5.0 s** down to **20–100 ms** ([DataVo vs SQLite](/manual/performance/datavo-vs-sqlite)).

## 1. Typed Single-Table Path

Accepts single-table queries whose `WHERE` clause combines `AND`, `OR`, `NOT`, column-vs-value comparisons (`=`, `<>`, `<`, `<=`, `>`, `>=`), `IS [NOT] NULL`, and `LIKE` / `NOT LIKE`, with any combination of aggregates, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, and `OFFSET`.

| Access Method | When Selected | Mechanism |
| --- | --- | --- |
| **Index point lookup** | Equality on PK or single-column B-tree index | Reads matching row keys in one index lookup, then fetches those rows by key. |
| **Primary-key range seek** | `BETWEEN` or `>=` / `<` on integer PK | On LSM, seeks SSTables and reads sequentially. On InMemory, binary-searches a lazily built sorted key copy (falls back to scan if range > 25% of table). |
| **SIMD columnar aggregate** | `COUNT`/`SUM`/`AVG`/`MIN`/`MAX` without `GROUP BY` on `[Columnar]` numeric columns | Reads 4,096-row columnar chunks with SIMD kernels; LSM skips chunks via `min`/`max`/`NULL`/`NaN` zone maps. |
| **Index-driven `GROUP BY`** | Whole-table `GROUP BY` on one indexed integer key with `COUNT(*)` / `COUNT(key)` | Streams finished groups in key order from the index without building a hash table. |
| **Full typed scan** | All other accepted predicates | Decodes only referenced columns in storage order; evaluates `WHERE` before materializing matches. |

Key implementation details:

- **Zero-allocation UTF-8 text matching:** On LSM, `=`, `<>`, `LIKE`, `NOT LIKE`, `IS [NOT] NULL`, and single-column text `GROUP BY` compare stored UTF-8 bytes directly without allocating `.NET` strings (falling back to UTF-16 decode only for `_` wildcards against non-ASCII text).
- **Bounded top-k sorting:** `ORDER BY ... LIMIT k` without a usable index compares sort-key cells in place against a bounded list of `k` winners, copying keys only when a row enters the top `k`.
- **Floating-point determinism on LSM:** `SUM` and `AVG` over `FLOAT`/`DOUBLE` columns are intentionally routed to the row scan when an LSM columnar index has an unflushed tail, guaranteeing bit-for-bit identical floating-point addition order. Integer sums, `COUNT`, `MIN`, and `MAX` are always pushed.

## 2. Typed Join Path

Accepts `INNER` and `LEFT` joins of any number of tables on integer equality keys, including per-table `WHERE` predicates (`OR`, `LIKE`), aggregates with or without `GROUP BY`, and top-N projections.

- **Hash join:** Streams the largest table (for all-`INNER` joins) or the `FROM` table (when a `LEFT JOIN` must preserve unmatched rows). Other tables are hashed on their join key, keeping only post-join columns. `INNER` filters apply during build; `LEFT` filters evaluate after NULL padding.
- **Key-only bit sets (`DenseInt64Set`):** When a joined table contributes only its key (such as `LEFT JOIN ... WHERE c.Id IS NULL` or existence checks) and keys are distinct and dense (≤ 64 span per key), DataVo packs them into a compact bit set (12.5 KB for 100,000 keys instead of a 2.9 MB hash table).
- **Index-driven probe:** For all-`INNER` joins where the hashed side is selective, a 32-key sample estimates fan-out; if ≤ 12.5% of the streamed table matches, DataVo probes via its single-column index in key order.

## 3. Hybrid Vector & General Fallback

- **Hybrid Vector Path:** Handles `ORDER BY embedding <=> @q LIMIT k` (and `<->`) with optional `WHERE` filters and joins. Over-fetches candidates by `LIMIT / selectivity`, evaluates typed predicates directly on stored candidate rows, and cost-routes HNSW searches to an exact SIMD Flat scan whenever the widened beam exceeds the graph crossover threshold. See [Vector Search](/manual/fundamentals/vector-search).
- **General Evaluator:** Executes `SELECT DISTINCT`, CTEs, computed select-list expressions, window functions (`RANK() OVER (...)`), `RIGHT`/`FULL`/`CROSS` joins, text/GUID joins, cross-table `OR` predicates, and joins inside explicit transactions. Single-table reads inside an explicit transaction remain on the typed scan over the MVCC snapshot.

## Compiled Queries & Planner Limits

Source-generated `[DataVoQuery]` methods and intercepted `DataVoTable<T>` LINQ queries bypass SQL parsing entirely and compile directly to typed byte-reading kernels (see [Compiled Scan Kernels](/manual/internals/compiled-scan-kernels)).

What the planner does **not** do:

- It does not keep persistent table statistics (index-probe decisions sample keys at runtime).
- It does not reorder arbitrary multi-way joins beyond selecting the streamed table and index probes.
- It does not use secondary indexes to satisfy `ORDER BY` or non-counting `GROUP BY` shapes.

## See Also
- [Zero-Allocation Row Model](/manual/internals/zero-allocation-rows) — Stack-only `RowRef` scans over stored bytes
- [Compiled Scan Kernels](/manual/internals/compiled-scan-kernels) — How Roslyn interceptors compile LINQ predicates into storage loops
- [DataVo vs SQLite](/manual/performance/datavo-vs-sqlite) — Measured latency across 28 SQL shapes and 5 hybrid vector queries
