# Vector DB + HNSW Roadmap

> Last updated: 2026-03-22  
> Status: Planned (post EF baseline)  
> Scope: Deliver production-usable vector search first, then HNSW as ANN acceleration.

## Goal

Add first-class vector search to DataVo with:

1. Correct brute-force similarity search as the baseline (exact nearest-neighbor)
2. Persistent HNSW indexing for fast approximate nearest-neighbor (ANN)

This keeps correctness and SQL UX stable before optimization complexity.

## Done criteria

### MVP done

- vector column type persisted and round-trippable
- SQL query surface for exact vector search (brute-force)
- deterministic correctness tests (memory + disk)
- benchmark harness with baseline latency/recall metrics

### HNSW done

- persistent HNSW index integrated with catalog/index manager
- SQL query path that can choose exact vs ANN mode
- ANN recall/latency benchmark targets met
- rebuild/recovery behavior validated on disk + WAL replay scenarios

## Phase roadmap

### Phase 0 — Architecture freeze

- finalize vector representation and dimensionality rules
- finalize SQL syntax for vector distance + ANN hints
- define persistent index metadata format for HNSW

Exit criteria:

- short RFC merged in docs
- no open questions on storage layout or SQL shape

### Phase 1 — Vector type + exact search (MVP)

- add `VECTOR(n)` support to parser/DDL/type validation
- add serializer support (disk + memory parity)
- implement distance functions (`L2`, `COSINE`, optional `DOT`)
- support exact top-k query pattern via `ORDER BY distance(...) LIMIT k`

Exit criteria:

- exact top-k returns correct nearest neighbors on deterministic fixtures
- in-memory and disk test paths both pass

### Phase 2 — SIMD acceleration (exact path)

- add SIMD kernels for `L2` and `COSINE`
- keep scalar fallback where SIMD is unavailable
- validate SIMD/scalar parity via tolerance-based tests

Exit criteria:

- measurable speedup over scalar baseline in benchmarks
- parity with scalar results

### Phase 3 — HNSW index (ANN)

- add ANN index abstraction in index manager
- implement persistent HNSW node/edge/layer storage
- add build strategy (offline full build + incremental maintenance)
- integrate ANN query execution with exact fallback when index is missing/ineligible

Exit criteria:

- ANN latency/recall tradeoff is configurable and benchmarked
- restart/recovery preserves usable index state
- exact fallback remains automatic and safe

### Phase 4 — Operational hardening

- validate WAL/recovery for vector + ANN updates
- add index rebuild tooling
- add corruption detection + fallback to exact path
- define memory/cost guardrails

Exit criteria:

- recovery and reliability tests pass
- runbook/documentation for operations is published

## SQL/API shape (proposed)

### DDL

- `CREATE TABLE Items (Id INT PRIMARY KEY, Embedding VECTOR(768));`
- `CREATE VECTOR INDEX IX_Items_Embedding ON Items(Embedding) USING HNSW;`

### Query (exact)

- `SELECT Id, L2_DISTANCE(Embedding, @query) AS Score FROM Items ORDER BY Score ASC LIMIT 10;`

### Query (ANN)

- same shape; planner picks HNSW when eligible
- optional hint in later slice (for example, `ANN(ef_search=128)`)

## Test and benchmark matrix

### Tests

- deterministic nearest-neighbor correctness fixtures
- storage roundtrip in disk + memory
- create/drop/rebuild/update/delete index consistency
- restart and WAL replay resilience
- ANN recall compared to exact baseline

### Benchmarks

- dataset sizes: 10k / 100k / 1M
- dimensions: 128 / 384 / 768
- k: 10 / 50
- latency p50/p95, recall@k, build time, memory footprint

## Key risks and mitigations

1. Graph memory growth → conservative defaults for `M` and dimension limits
2. Write amplification → start synchronous, add batch/background mode later
3. Format lock-in → version index file format from v1
4. Planner complexity → explicit ANN eligibility checks with exact fallback

## Commit sequence (implementation-ready)

### Milestone A — Vector MVP

- `feat(core): add VECTOR type and serializer support`
- `feat(core): add vector distance functions and exact top-k query path`
- `test(core): add vector correctness and storage roundtrip coverage`
- `bench(core): add exact vector search benchmarks`

### Milestone B — SIMD

- `feat(core): add SIMD kernels for vector distance`
- `test(core): validate SIMD/scalar parity`
- `bench(core): compare scalar vs SIMD performance`

### Milestone C — HNSW ANN

- `feat(core): add HNSW index metadata and persistence model`
- `feat(core): integrate HNSW query execution with exact fallback`
- `test(core): add ANN recall and recovery coverage`
- `bench(core): add HNSW latency-recall benchmarks`

### Milestone D — Hardening

- `feat(core): add vector/hnsw rebuild and recovery tooling`
- `test(core): add crash/restart reliability coverage`
- `docs(core): publish vector and hnsw operational guide`

## Immediate next actions

1. open RFC PR for syntax + storage freeze
2. implement Vector MVP before ANN indexing
3. add benchmark fixtures early to prevent unmeasured optimization
4. defer planner heuristics until exact + ANN paths are both stable
