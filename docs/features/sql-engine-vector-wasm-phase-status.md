# SQL -> Vector -> WASM Phase Status

_Last updated: 2026-03-25_

This status table tracks the active multi-phase plan execution in one place.
Percentages are practical delivery estimates (implementation + test confidence), not formal completion gates.

| Phase | Short title | Done % | Remaining % | Notes |
|---|---|---:|---:|---|
| 1.1 | Advanced predicates | 100 | 0 | `IN`, `BETWEEN`, `LIKE` implemented and tested. |
| 1.2 | Set operations | 100 | 0 | `UNION` / `UNION ALL` implemented with coverage. |
| 1.3 | Subqueries | 100 | 0 | Uncorrelated + scalar/EXISTS supported; correlated paths explicitly rejected. |
| 1.4 | Schema evolution | 100 | 0 | `ALTER TABLE ADD/DROP/MODIFY` supported with intentional safety guardrails. |
| 1.5 | Publishability hardening | 78 | 22 | Pack flow works for `DataVo.Core` + `DataVo.Data`; local consumer smoke-check script now validates install/build from artifacts. Remaining API polish + sample app validation. |
| 2 | Relational hardening | 45 | 55 | Major isolation improvements landed; remaining concurrency and index-ordering hardening passes needed. |
| 3.1 | Vector type + storage | 95 | 5 | Core vector storage/query path in place. |
| 3.2 | SIMD distance kernels | 85 | 15 | SIMD path is present; remaining validation/benchmark maturity work. |
| 3.3 | Vector query surface | 95 | 5 | Practical query surface is active and benchmarked. |
| 3.4 | ANN indexing (HNSW) | 90 | 10 | Strongly implemented; final production hardening/tuning still ongoing. |
| 4.1 | WASM runtime baseline | 95 | 5 | Browser runtime works and is testable. |
| 4.2 | Browser storage abstraction | 90 | 10 | Worker/fallback abstraction is implemented; remaining cleanup and stabilization. |
| 4.3 | Persistent browser storage | 80 | 20 | Durable commit protocol and browser tests exist; Release runtime stabilization remains. |

## Deferred automation reminder

- Keep this as a tracked post-phase item: automatically generate Playwright browser scenarios from selected .NET E2E tests, with opt-out metadata for host/runtime-specific cases.
