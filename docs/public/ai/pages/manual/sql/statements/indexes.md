# CREATE INDEX

> Source route: /manual/sql/statements/indexes
> Source file: manual/sql/statements/indexes.md

`CREATE INDEX` builds a secondary index on an existing table in the active database and populates it immediately from current rows. DataVo supports four index methods: `BTREE` for scalar lookups and ranges, `HNSW` and `FLAT` for vector similarity search, and `COLUMNAR` for vectorized analytical aggregations.

## Syntax

```sql
CREATE INDEX <index_name> ON <table_name> (<column_name> [, <column_name>, ...])
    [USING BTREE | HNSW | FLAT | COLUMNAR]
    [WITH (<option_name> = <value> [, ...])];
```

::: info DataVo Extension
`USING HNSW`, `USING FLAT`, `USING COLUMNAR`, and the `WITH (...)` index-tuning clause are DataVo extensions beyond standard SQLite and ANSI SQL.
:::

## Parameters

| Clause | Required | Description |
| --- | --- | --- |
| `<index_name>` | Yes | Unique index name for `<table_name>`. |
| `ON <table_name>` | Yes | Existing table in the active database to index. |
| `(<column_name>, ...)` | Yes | One or more target columns. `HNSW` and `FLAT` require exactly one `VECTOR(n)` column; `BTREE` and `COLUMNAR` accept one or more columns. |
| `USING <method>` | No | Index implementation: `BTREE` (default when omitted), `HNSW`, `FLAT`, or `COLUMNAR`. |
| `WITH (<opt> = <val>, ...)` | No | Key-value tuning options for vector indexes (`HNSW` and `FLAT`). Option names ignore case and underscores. |

::: warning Syntax Notes in v0.5
`CREATE INDEX` does **not** support `UNIQUE` or `IF NOT EXISTS` in DataVo `v0.5`. To enforce uniqueness on a column, declare `UNIQUE` inside [`CREATE TABLE`](/manual/sql/statements/create-table).
:::

## Index Methods Overview

| Method | Syntax | Target Columns | Best For |
| --- | --- | --- | --- |
| `BTREE` (default) | `USING BTREE` (or omit `USING`) | Single or composite scalar columns | Equality lookups, range scans, selective join probes, and single-key integer `GROUP BY COUNT`. |
| `HNSW` | `USING HNSW [WITH (...)]` | Single `VECTOR(n)` column | Fast approximate nearest-neighbor (ANN) search on medium and large embedding tables. |
| `FLAT` | `USING FLAT [WITH (metric = ...)]` | Single `VECTOR(n)` column | Exact exhaustive vector search on small tables and recall benchmarks. |
| `COLUMNAR` | `USING COLUMNAR` | Numeric (`INT`, `BIGINT`, `FLOAT`, `DOUBLE`), `BIT`, `DATE`, and `DATETIME` columns | Vectorized `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX` aggregations without decoding full rows. |

## Scalar B-Tree Indexes (`USING BTREE`)

B-Tree is the default index method when `USING` is omitted. You can index a single scalar column or a composite list of columns:

```sql
-- Single-column B-Tree index
CREATE INDEX ix_players_score ON players (score);

-- Explicit USING BTREE on a composite key
CREATE INDEX ix_players_region_score ON players (region, score) USING BTREE;
```

### Behavior & Query Acceleration

- **`NULL` keys:** A row whose key has a `NULL` in any indexed column never matches an equality or range lookup, so `IS NULL` predicates scan table rows and multiple `NULL`s may coexist in a `UNIQUE` column. `Lsm` indexes still record such rows under a dedicated `NULL` key (every `INSERT`, `UPDATE`, `DELETE`, `COMMIT` and rebuild path does this the same way), so a counting `GROUP BY` on an indexed integer column gets its `NULL` group from the index without reading the table. `InMemory` indexes hold no entry for them and count that group from the rows' null flags.
- **When B-Tree accelerates queries:**
  - Single-column equality predicates (`WHERE score = 500`) use an index point lookup.
  - Selective `INNER JOIN` probes on indexed integer keys probe the B-Tree instead of streaming the full table.
  - Whole-table `GROUP BY <indexed_int_col>` with `COUNT(*)` or `COUNT(col)` streams groups directly in key order from the index.
  - Secondary B-Tree indexes are **not** used to satisfy general `ORDER BY` clauses or non-counting `GROUP BY` shapes (see [Query Planner](/manual/internals/query-planner)).

## Vector Indexes (`USING HNSW` & `USING FLAT`)

Vector indexes require **exactly one** `VECTOR(n)` column (`VECTOR(n)`, `VEC2`, `VEC3`, `VEC4`, `FLOAT[n]`, or `REAL[n]`). Rows where the vector column is `NULL` are skipped.

```sql
-- Approximate HNSW index for text embeddings
CREATE INDEX ix_documents_emb ON documents (embedding)
USING HNSW WITH (
    metric = 'cosine',
    m = 16,
    ef_construction = 64,
    ef_search = 64,
    enable_diversity_heuristic = true
);

-- Exact FLAT index for 3D spatial coordinates
CREATE INDEX ix_entities_pos ON world_entities (position)
USING FLAT WITH (metric = 'euclidean');
```

### Vector `WITH (...)` Options

Passing HNSW graph options (`m`, `ef_construction`, `ef_search`, `enable_diversity_heuristic`) to `FLAT` or `BTREE`, passing `metric` to `BTREE`, or specifying out-of-range values throws a `BindingException` and creates no index. Option names ignore case and underscores (for example, `efConstruction` and `ef_construction` are equivalent).

| Option | Default | Valid Range | Applies To | Description |
| --- | --- | --- | --- | --- |
| `metric` | `'cosine'` | `'cosine'`, `'euclidean'` (alias `'l2'`) | `HNSW`, `FLAT` | Distance metric persisted in the catalog. Use `'cosine'` for normalized embeddings and `'euclidean'`/`'l2'` for spatial positions. |
| `m` | `16` | `2` – `128` | `HNSW` | Maximum bi-directional links per graph layer (doubled on layer 0). Higher values improve recall at higher memory and build cost. |
| `ef_construction` | `64` | `1` – `4096` | `HNSW` | Candidate beam width during graph construction. Higher values yield better graph quality at slower build speed. |
| `ef_search` | `64` | `1` – `4096` | `HNSW` | Minimum candidate beam width during nearest-neighbor search. |
| `enable_diversity_heuristic` | `true` | `true`, `false` | `HNSW` | Preserves cross-cluster links during neighbor selection; disable only for faster builds on uniform data. |

### Filtered Vector Search Behavior

For hybrid queries combining `WHERE` predicates with `ORDER BY embedding <=> @q LIMIT k` (or `<->`), DataVo automatically widens the candidate beam and switches to an exact SIMD scan when graph traversal would cost more than scanning stored vectors, ensuring filtered `LIMIT k` queries return all `k` matching rows whenever enough rows qualify.

## Columnar Indexes (`USING COLUMNAR`)

Columnar indexes store selected columns in compact typed column chunks so analytical aggregates avoid reading full row payloads.

```sql
CREATE INDEX cx_players_stats ON players (score, id) USING COLUMNAR;
```

### Eligibility & Storage Rules

- **Allowed column types:** `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `BIT`, `DATE`, and `DATETIME` only (a `DATETIME` is kept as its UTC ticks and scanned with the 64-bit integer kernels; `MIN`/`MAX`/`COUNT` fold natively). Any other type (such as `VARCHAR`, `GUID`, or `VECTOR`) is rejected at `CREATE INDEX` time.
- **Storage modes:**
  - `InMemory`: Stores 4–8 bytes per row per indexed column in RAM for SIMD-accelerated scans.
  - `Lsm`: Persists format-v8 columnar segments inside `.sst` files with `min`/`max`/`NULL`/`NaN` zone maps for chunk skipping.
- **Query eligibility:** Accelerates `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX` queries **without `GROUP BY`** whose `WHERE` clause is an `AND` of column-to-literal comparisons or `IS NULL` checks over indexed columns. Queries fall back cleanly to row scans when:
  - Running inside an explicit `BEGIN` ... `COMMIT` transaction.
  - Using `GROUP BY`.
  - Filtering on comparisons where precision could differ (such as a fractional literal against an `INT` column), `BIT`/`DATE` predicates, or `NaN` floats.
  - Computing `SUM` or `AVG` over `FLOAT`/`DOUBLE` on an `Lsm` table with an unflushed memtable tail (to preserve deterministic floating-point addition order).
- **Consolidation & scope:** Columnar indexes never serve point lookups and do not replace B-Tree indexes. Multiple `USING COLUMNAR` indexes on the same table are consolidated internally.

::: tip Choosing the Right Index
- Use **`BTREE`** for selective `WHERE col = ...` filters and join keys.
- Use **`HNSW`** for `VECTOR(n)` tables with thousands or millions of rows; use **`FLAT`** for small tables or exact recall baselines.
- Use **`COLUMNAR`** for dashboard rollups (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) over numeric columns.
:::

## Dropping an Index

Remove any index using [`DROP INDEX`](/manual/sql/statements/alter-and-drop#drop-index):

```sql
DROP INDEX ix_players_score ON players;
```

## See Also

- [ALTER TABLE & DROP](/manual/sql/statements/alter-and-drop): Dropping indexes (`DROP INDEX ... ON <table>`) and altering indexed columns
- [Vector Search](/manual/fundamentals/vector-search): Embeddings, `<=>` / `<->` distance operators, and hybrid vector queries
- [Query Planner](/manual/internals/query-planner): How DataVo routes queries to B-Tree, HNSW, FLAT, and Columnar fast paths
- [EXPLAIN](/manual/sql/statements/explain): Inspecting logical and physical execution plans
