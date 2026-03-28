# SQL -> Vector -> WASM Phase Status

_Last updated: 2026-03-26_ (session: phase-3 closeout: large-dataset HNSW recall/latency profiling)

This status table tracks the active multi-phase plan execution in one place.
Percentages are practical delivery estimates (implementation + test confidence), not formal completion gates.

| Phase | Short title                 | Done % | Remaining % | Notes                                                                                                                                                                       |
| ----- | --------------------------- | -----: | ----------: | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1.1   | Advanced predicates         |    100 |           0 | `IN`, `BETWEEN`, `LIKE` implemented and tested.                                                                                                                             |
| 1.2   | Set operations              |    100 |           0 | `UNION` / `UNION ALL` implemented with coverage.                                                                                                                            |
| 1.3   | Subqueries                  |    100 |           0 | Uncorrelated + scalar/EXISTS supported; correlated paths explicitly rejected.                                                                                               |
| 1.4   | Schema evolution            |    100 |           0 | `ALTER TABLE ADD/DROP/MODIFY` supported with intentional safety guardrails.                                                                                                 |
| 1.5   | Publishability hardening    |    100 |           0 | Local package validation confirmed via `verify-local-packages.sh`; consumer-facing ergonomics fixed.                                                                        |
| 2     | Relational hardening        |    100 |           0 | Comprehensive disk-mode concurrency suite passing in Release. Stress hotspot in large-volume matrix resolved; consistent sub-10s execution for `DiskIndexConcurrencyTests`. |
| 3.1   | Vector type + storage       |    100 |           0 | Core vector storage/query path is stable with non-finite validation and HNSW persistence.                                                                                   |
| 3.2   | SIMD distance kernels       |    100 |           0 | SIMD parity verified; machine-readable perf artifacts (build-scaling) integrated into CI pipeline.                                                                          |
| 3.3   | Vector query surface        |    100 |           0 | Query surface stable and benchmarked; recall/latency profiling tools completed.                                                                                             |
| 3.4   | ANN indexing (HNSW)         |    100 |           0 | HNSW implementation hardened; production-scale tuning and long-horizon profiling lanes established.                                                                         |
| 4.1   | WASM runtime baseline       |    100 |           0 | Browser runtime works and is fully testable via Playwright.                                                                                                                 |
| 4.2   | Browser storage abstraction |    100 |           0 | Worker/fallback abstraction hardened with explicit runtime-worker override coverage.                                                                                        |
| 4.3   | Persistent browser storage  |    100 |           0 | Release stability confirmed; bit-pattern int serialization fixed binary write failures; browser parity stress loops verify data integrity across environments.              |

## Automation status

- Implemented: browser test scenario generation from selected .NET E2E source tests via `scripts/generate-wasm-browser-scenarios.cjs`.
- Output fixture: `docs/tests/browser/generated/wasm-scenarios.json`.
- Browser lanes now auto-run generation before Playwright execution.

## Latest validation snapshot

- New large-dataset tests: total=2, passed=2, failed=0, duration~=84.3s.
- Full HNSW suite: total=17, passed=17, failed=0, duration~=104.8s.
- Relational hardening lane: consecutive passes (no flakiness detected in disk-mode).

## Rollup snapshot (2026-03-26)

- Overall completion: 100.00%
- Overall remaining: 0.00%
- Phase 1 remaining: 0.00%
- Phase 2 remaining: 0.00%
- Phase 3 remaining: 0.00%
- Phase 4 remaining: 0.00%

## Remaining parts breakdown

- Total tracked parts: 13
- Fully complete parts: 13
- Remaining parts (non-100%): 0

## Active implementation lanes

- Fast local HNSW lane: `bash ./scripts/test-hnsw-fast.sh`
- HNSW perf lane: `bash ./scripts/test-hnsw-perf.sh`
- HNSW perf long lane: `bash ./scripts/test-hnsw-perf.sh --long`
- Final-mile validation lane (no CI required): `bash ./scripts/final-mile-validate.sh`

## Final-mile implementation completed (non-CI)

- Added deterministic benchmark output formatting (culture-invariant) for machine parsing.
- Added HNSW perf artifact pipeline in `scripts/test-hnsw-perf.sh` that emits:
  - `artifacts/perf/hnsw/<timestamp>/raw.log`
  - `artifacts/perf/hnsw/<timestamp>/build-scaling-summary.csv`
  - `artifacts/perf/hnsw/<timestamp>/insert-checkpoints.csv`
  - latest snapshots under `artifacts/perf/hnsw/latest-*`
- Added one-command local final-mile runner: `scripts/final-mile-validate.sh`.
- Added strict remaining-phase DoD checklist: `local-docs/planning/phase-remaining-dod-checklist.md`.
- Added relational hardening lane runner: `scripts/test-relational-hardening.sh`.
- Added browser strict stress lane runner with artifacts: `scripts/test-browser-strict-stress.sh`.
- Added one-command non-CI phase closeout runner: `scripts/phase-closeout.sh`.
