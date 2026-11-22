# Limits & Unsupported Features

> Source route: /manual/reference/limits
> Source file: manual/reference/limits.md

This page documents the explicit architectural boundaries and unsupported features of DataVo `v0.4` (`0.4.0-rc.1`) so you can evaluate fit without guessing from source code or roadmap notes.

## Architectural & Storage Limits

| Boundary | Behavior in `v0.4` (`0.4.0-rc.1`) |
| --- | --- |
| **Process ownership** | Single-process write ownership enforced via `datavo.lock`. Multiple contexts/connections inside one process share one engine; a second process opening the same directory fails with `DV7001_DATABASE_IN_USE`. |
| **Deployment model** | Embedded in-process only. No wire protocol server, replication daemon, or HA clustering (`DataVo.Server` for PostgreSQL-wire GUI clients like DBeaver/DataGrip is unscheduled). |
| **Prerelease storage format** | On-disk structures may evolve before `1.0`. Maintain seed or export/import scripts across prerelease upgrades. |
| **Vector dimensions** | Fixed per column at `CREATE TABLE` (`VECTOR(n)`). Inserted vectors must match the declared dimension. |
| **LSM MemTable flush size** | Fixed internal constant of 32 MB (not a `DataVoConfig` setting). |
| **Transaction isolation** | Snapshot isolation with first-committer-wins conflict detection. Full serializable isolation and predicate-range phantom prevention are **not** provided (write skew is possible). |
| **HNSW vector recall** | HNSW is approximate; measure recall on your own embeddings or use `USING FLAT` / `exact: true` when exact nearest neighbors are required. |

## Query-Shape Performance Limits

Correctness does not depend on query shape, but allocation and latency do. Queries outside the compiled/typed fast paths execute on the general dictionary evaluator, which allocates a dictionary per row read (seconds and gigabytes on million-row tables):

- `SELECT DISTINCT`
- `RIGHT`, `FULL`, and `CROSS` joins
- Joins on text (`VARCHAR`) or `GUID` keys
- `OR` predicates spanning two joined tables
- Computed expressions in the `SELECT` list
- Window functions (`RANK()`) and common table expressions (CTEs)
- Any SQL query executed inside an explicit `BEGIN ... COMMIT` transaction

See [Query Planner](/manual/internals/query-planner) for how to stay on zero-allocation fast paths.

## SQL `DATE` Contract

DataVo provides a calendar `DATE` type (`YYYY-MM-DD`) and has **no** `DATETIME` or `TIMESTAMP` column type:

- Store timestamps or points in time as `BIGINT` (UTC ticks or Unix milliseconds).
- In C# models and source-generated projections, SQL `DATE` maps to `DataVo.Core.DataVoDate` across all targets, and additionally supports `System.DateOnly` on `.NET 10`.
- `System.DateTime` properties are not mapped to SQL `DATE`.

## Unsupported Features

| Category | Not Supported in `v0.4` (`0.4.0-rc.1`) |
| --- | --- |
| **PostgreSQL compatibility** | Not PostgreSQL-compatible. No PostgreSQL wire protocol, no `pg_class`/`pg_attribute` system catalogs, and no PostgreSQL role catalog. |
| **SQL clauses & expressions** | No views (`CREATE VIEW`), `INTERSECT`, `EXCEPT`, `CASE WHEN`, `CAST`, `COALESCE`, `PRAGMA`, date/math SQL functions, or window functions other than `RANK()`. |
| **Procedural SQL** | No stored procedures, functions, or triggers. |
| **Text collation** | Ordinal comparison only; `LIKE` is case-sensitive. |
| **Vector operators & functions** | No `<#>` inner-product operator; no core SQL `COSINE_DISTANCE()`/`L2_DISTANCE()` functions (only `<->` L2 and `<=>` Cosine operators; function shims exist only in EF Core preview). |
| **EF Core bridge** | No EF migrations, shadow properties, complex LINQ joins, broad `GroupBy` translation, or Native AOT support. |
| **ADO.NET** | No `DbProviderFactory`, no connection pooling, no async cancellation via `CommandTimeout`/`Cancel()`. |
| **WebAssembly / npm** | `DataVo.Browser` is an experimental playground runtime for documentation only; no production npm package is published. |

## See Also

- [SQL Compatibility](/manual/sql/compatibility) — Full SQL dialect support matrix
- [SQL Data Types](/manual/sql/data-types) — Supported column types and C# mappings
- [Transactions & MVCC](/manual/fundamentals/transactions-and-mvcc) — Snapshot isolation contract and write-conflict behavior
- [Query Planner](/manual/internals/query-planner) — Fast paths vs. general evaluator
