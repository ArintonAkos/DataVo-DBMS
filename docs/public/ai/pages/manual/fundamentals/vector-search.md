# Vector Search

> Source route: /manual/fundamentals/vector-search
> Source file: manual/fundamentals/vector-search.md

DataVo stores dense float embeddings directly alongside relational columns in `VECTOR(n)` columns and queries them with `<=>` (Cosine) and `<->` (L2 / Euclidean) distance operators. Embeddings live in the same table, index lifecycle, and transaction log as your application metadata.

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
db.Execute("CREATE DATABASE Docs; USE Docs;");

db.Execute("""
CREATE TABLE documents (
    id INT PRIMARY KEY,
    title VARCHAR,
    category VARCHAR,
    embedding VECTOR(3)
);
""");

db.Execute("INSERT INTO documents VALUES (1, 'Intro', 'guide', '[1, 0, 0]')");
db.Execute("INSERT INTO documents VALUES (2, 'LSM',   'guide', '[0.9, 0.1, 0]')");
db.Execute("INSERT INTO documents VALUES (3, 'Sync',  'cloud', '[0, 1, 0]')");
db.Execute("CREATE INDEX idx_docs_emb ON documents (embedding) USING FLAT");

var top = db.Execute("""
SELECT id, title, embedding <=> '[1, 0, 0]' AS distance
FROM documents
ORDER BY distance ASC
LIMIT 2;
""")[0];
```

*(The snippet uses `VECTOR(3)` for readability; production schemas use `VECTOR(384)`, `VECTOR(1536)`, etc.)*

::: info DataVo Extension
`VECTOR(n)` (along with aliases `VEC2`, `VEC3`, `VEC4`, `FLOAT[n]`, and `REAL[n]`), the `<=>` and `<->` distance operators, and `USING HNSW` / `USING FLAT` index clauses are DataVo SQL extensions beyond standard ANSI SQL.
:::

## Distance Operators

DataVo ranks closer vectors by **smaller** distance values, so nearest-neighbor queries order ascending (`ORDER BY distance ASC`) with a `LIMIT`:

| Operator | Metric | Typical Use Case |
| --- | --- | --- |
| `<=>` | Cosine distance | Text and semantic embeddings where vector direction matters more than magnitude |
| `<->` | L2 (Euclidean) distance | Geometric spaces where absolute coordinate distance matters |

## Choosing a Vector Index: `FLAT` vs `HNSW`

Create an index on the `VECTOR(n)` column to accelerate similarity search:

```sql
-- Exact SIMD scan (100% recall, ideal for tests and smaller tables)
CREATE INDEX idx_docs_flat ON documents (embedding) USING FLAT;

-- Approximate nearest-neighbor graph (fast on large tables)
CREATE INDEX idx_docs_hnsw ON documents (embedding)
USING HNSW WITH (
    metric = 'cosine',
    m = 24,
    ef_construction = 128,
    ef_search = 96,
    enable_diversity_heuristic = true
);
```

All `WITH (...)` options are optional and ignore case and underscores (`ef_construction` and `EfConstruction` are identical):

| Option | Default | Range | Effect |
| --- | --- | --- | --- |
| `metric` | `cosine` | `cosine`, `euclidean` (`l2`) | Distance metric ranked by the index (also supported on `FLAT`) |
| `m` | `16` | `2..128` | Max neighbor links per graph layer (`2 * m` on layer 0) |
| `ef_construction` | `64` | `1..4096` | Candidate beam width during index build |
| `ef_search` | `64` | `1..4096` | Minimum candidate beam width at query time |
| `enable_diversity_heuristic` | `true` | `true`, `false` | Preserves inter-cluster links; keep `true` for clustered text embeddings |

Options are persisted in the catalog and reused if the graph is rebuilt after a crash or reopen. Passing an unknown option, an out-of-range value, or an HNSW option on a `FLAT`/B-tree index raises an error.

## Filtered & Joined Vector Search

Combine vector ranking with `WHERE` predicates, distance thresholds, or `JOIN`s:

```sql
SELECT id, title, embedding <=> '[1, 0, 0]' AS distance
FROM documents
WHERE category = 'guide' AND embedding <=> '[1, 0, 0]' < 0.25
ORDER BY distance ASC
LIMIT 10;
```

DataVo guarantees the requested `LIMIT` whenever enough matching rows exist:

1. **Adaptive candidate expansion:** DataVo requests `~LIMIT / selectivity` candidates from the vector index, tests scalar or joined filters on stored rows, and expands the candidate count if too few rows pass.
2. **Exact-scan cost routing:** When a selective filter widens the HNSW beam past the break-even point (~9% of vectors at 384D, ~20% at 1,536D), HNSW automatically switches to an exact SIMD scan, improving both latency and recall. (Set `HNSWIndex.EnableExactScanCostRouting = false` in C# to disable.)
3. **Exact table-scan fallback:** If expansion still finds fewer than `LIMIT` passing rows, DataVo filters the table first and ranks only the surviving rows against the index's stored vectors.

## C# Direct Vector Search (`SearchNearest`)

When you already have a `float[]`, `ReadOnlySpan<float>`, or vector string in C# and want to query a named index without building SQL text, call `db.SearchNearest(...)`:

```csharp
ReadOnlySpan<float> query = [1f, 0f, 0f];
List<Dictionary<string, object?>> hits = db.SearchNearest(
    tableName: "documents",
    indexName: "idx_docs_flat",
    queryVector: query,
    topK: 5);
```

::: details API Reference
```csharp
public List<Dictionary<string, object?>> SearchNearest(
    string tableName, string indexName, float[] queryVector, int topK = 10);

public List<Dictionary<string, object?>> SearchNearest(
    string tableName, string indexName, ReadOnlySpan<float> queryVector, int topK = 10);

public List<Dictionary<string, object?>> SearchNearest(
    string tableName, string indexName, string queryVector, int topK = 10);
```
:::

## See Also

- [SQL Indexes (`CREATE INDEX`)](/manual/sql/statements/indexes): Full DDL syntax for `BTREE`, `HNSW`, `FLAT`, and `COLUMNAR` indexes
- [SQL Data Types](/manual/sql/data-types): `VECTOR(n)` column declarations and literal syntax
- [LSM & Durability](/manual/fundamentals/lsm-and-durability): How HNSW snapshots and FLAT indexes recover on reopen
