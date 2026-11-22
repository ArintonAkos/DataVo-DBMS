# Indexes

> Source route: /manual/sql/statements/indexes
> Source file: manual/sql/statements/indexes.md

DataVo supports four index methods via `CREATE INDEX`: B-Tree for scalar lookups and ranges, `HNSW` and `FLAT` for vector similarity search, and `COLUMNAR` for vectorized analytical aggregations.

```sql
-- Scalar B-Tree index (default)
CREATE INDEX ix_players_score ON players (Score);

-- Approximate vector index
CREATE INDEX ix_documents_emb ON documents (embedding)
USING HNSW WITH (metric = 'cosine', m = 16, ef_construction = 64, ef_search = 64);

-- Analytical columnar index
CREATE INDEX cx_players_stats ON players (Score, Id) USING COLUMNAR;

-- Drop an index
DROP INDEX ix_players_score ON players;
```

## Index Types Overview

| Method | Syntax | Target Columns | Best For |
| --- | --- | --- | --- |
| `BTREE` (default) | `USING BTREE` (or omit `USING`) | Single or composite scalar columns | Point lookups, range scans, and `ORDER BY` on scalar columns. |
| `HNSW` | `USING HNSW [WITH (...)]` | Single `VECTOR(n)` column | Fast approximate nearest-neighbor search on medium/large embedding tables. |
| `FLAT` | `USING FLAT [WITH (metric = ...)]` | Single `VECTOR(n)` column | Exact exhaustive vector search on small tables and recall benchmarks. |
| `COLUMNAR` | `USING COLUMNAR` | Numeric, `BIT`, and `DATE` columns | High-speed `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` without reading full rows. |

::: info DataVo Extension
`USING HNSW`, `USING FLAT`, and `USING COLUMNAR` (along with the `WITH (...)` tuning clause) are DataVo extensions beyond standard SQLite/ANSI SQL.
:::

## Scalar B-Tree Indexes

```sql
CREATE INDEX ix_players_region_score ON players (Region, Score);
```

- A scalar index stores only rows where **all** indexed columns are non-`NULL`. Rows with a `NULL` key receive no index entry, so `IS NULL` predicates scan rows and multiple `NULL`s may coexist in a `UNIQUE` column.

## Vector Indexes (`HNSW` & `FLAT`)

Both `HNSW` and `FLAT` accept `metric` in `WITH (...)`. Passing graph options (`m`, `ef_construction`, `ef_search`, `enable_diversity_heuristic`) to `FLAT` or `BTREE`, or passing out-of-range values, throws an error and creates no index. Option names ignore case and underscores.

| Option | Default | Valid Range | Applies To | Description |
| --- | --- | --- | --- | --- |
| `metric` | `'cosine'` | `'cosine'`, `'euclidean'` (or `'l2'`) | `HNSW`, `FLAT` | Distance metric persisted in the catalog. Use `'euclidean'` for 3D positions. |
| `m` | `16` | `2` – `128` | `HNSW` | Bi-directional links per graph layer (doubled on layer 0). |
| `ef_construction` | `64` | `1` – `4096` | `HNSW` | Candidate beam width during index build. |
| `ef_search` | `64` | `1` – `4096` | `HNSW` | Minimum candidate beam width during search. |
| `enable_diversity_heuristic` | `true` | `true`, `false` | `HNSW` | Keeps cross-cluster links; disable only for faster builds on uniform data. |

For filtered vector searches, DataVo widens the candidate beam automatically and switches to an exact scan when graph traversal would cost more than scanning stored vectors—ensuring filtered `LIMIT k` queries return the full `k` rows when enough rows match.

## Columnar Indexes (`USING COLUMNAR`)

```sql
CREATE INDEX cx_players ON players (Score, Id) USING COLUMNAR;
```

- **Allowed types:** `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `BIT`, and `DATE` only. Any other type is rejected.
- **Storage modes:** Accelerates `InMemory` (4–8 bytes/row/column in RAM) and `Lsm` (persisted as format-v8 columnar segments inside `.sst` files). Legacy `Disk` mode records the index but falls back to row scans.
- **Query eligibility:** Used for aggregates without `GROUP BY` whose `WHERE` clause is an `AND` of column-to-literal comparisons or `IS NULL` checks over indexed columns. Queries inside explicit transactions, queries with `GROUP BY`, or comparisons where precision could differ (e.g., fractional literal vs. `INT`, `BIT`/`DATE` predicates, or `NaN` floats) fall back to row scans.
- **Scope:** Never serves point lookups and does not replace a B-Tree index. Multiple columnar indexes on the same table are consolidated internally.

## `DROP INDEX`

```sql
DROP INDEX <index_name> ON <table_name>;
```

DataVo requires the `ON <table_name>` clause when dropping an index.

## See Also
- [Vector Search](/manual/fundamentals/vector-search) — End-to-end guide to embeddings, `SearchNearest`, and spatial queries
- [Data Types](/manual/sql/data-types) — `VECTOR(n)`, `VEC2`/`VEC3`/`VEC4`, and `FLOAT[n]` declarations
- [Query Planner](/manual/internals/query-planner) — When the planner selects B-Tree, HNSW, FLAT, or Columnar fast paths
