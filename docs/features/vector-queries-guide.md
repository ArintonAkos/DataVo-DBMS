# Vector Queries Guide

> Last updated: 2026-03-24
> Status: Active feature with HNSW support
> Audience: Application developers using vector/embedding queries

## Overview

DataVo provides first-class vector search capabilities for similarity-based queries on embedding data. This guide covers:

- Creating and querying vector columns
- Distance metrics and similarity functions
- Exact (brute-force) vs. approximate nearest-neighbor (ANN) search
- Hybrid queries that combine vectors with lexical filters and joins
- Performance optimization through indexing

## Vector columns

### Creating a vector column

Define vector columns using the `VECTOR(dimension)` type:

```sql
CREATE TABLE Embeddings (
    Id INT PRIMARY KEY,
    Content VARCHAR(500),
    Vector VECTOR(768),
    CreatedAt DATETIME,
    Status VARCHAR(20)
);
```

Vector dimensions are fixed per column. Common dimensions:

- `384` – Lightweight embeddings (e.g., all-MiniLM-L6-v2)
- `768` – Standard embeddings (e.g., sentence-transformers, GPT-3)
- `1536` – Large embeddings (e.g., OpenAI text-embedding-3-large)

### Valid vector values

Vectors are arrays of floating-point numbers:

```sql
INSERT INTO Embeddings VALUES
    (1, 'Hello world', '[0.1, 0.2, 0.3, ...]', '2026-03-20', 'active');

INSERT INTO Embeddings VALUES
    (2, 'Another embedding', '[0.05, 0.25, 0.28, ...]', '2026-03-21', 'active');
```

Constraints:

- All components must be finite (no `NaN` or `Infinity`)
- Dimension must match column definition exactly
- JSON array format: `[float, float, ..., float]`

### Invalid vectors (rejected)

```sql
-- ERROR: Contains NaN
INSERT INTO Embeddings VALUES (1, 'text', '[0.1, NaN, 0.3]', ..., ...);

-- ERROR: Dimension mismatch (768 expected, 3 provided)
INSERT INTO Embeddings VALUES (1, 'text', '[0.1, 0.2, 0.3]', ..., ...);

-- ERROR: Non-finite value
INSERT INTO Embeddings VALUES (1, 'text', '[0.1, Infinity, 0.3]', ..., ...);
```

## Distance metrics and similarity functions

DataVo supports two common distance metrics for nearest-neighbor search:

### Euclidean distance (L2)

Straight-line distance in vector space. Use when:

- Embeddings are normalized and magnitude matters
- You want scale-sensitive similarity

```sql
SELECT Id, Content,
       L2_DISTANCE(Vector, @query_vector) AS distance
FROM Embeddings
ORDER BY distance ASC
LIMIT 10;
```

**Distance interpretation**: Lower values = more similar

- `0.0` – Identical vectors
- `0.5` – Similar vectors
- `1.414+` – Dissimilar vectors

### Cosine distance

Measures angle between vectors (normalized). Use when:

- Embeddings are direction-based (e.g., all-MiniLM)
- Magnitude should be ignored
- You want fast similarity without normalization

```sql
SELECT Id, Content,
       COSINE_DISTANCE(Vector, @query_vector) AS distance
FROM Embeddings
ORDER BY distance ASC
LIMIT 10;
```

**Distance interpretation**: Lower values = more similar

- `0.0` – Identical direction
- `0.5` – Perpendicular or mildly similar
- `2.0` – Opposite direction

### Choosing a metric

| Metric   | Best for                               | Distance range |
| :------- | :------------------------------------- | :------------- |
| `L2`     | Magnitude + direction matter           | [0, ∞)         |
| `COSINE` | Direction only (normalized embeddings) | [0, 2]         |

If unsure, **use COSINE** for embeddings from transformer models (sentence-transformers, OpenAI, etc.).

## Exact nearest-neighbor queries

Exact search evaluates all rows in the table—best for correctness, lower latency on small datasets.

### Basic usage: top-k

```sql
-- Find 10 most similar embeddings to a query vector
SELECT Id, Content, Score,
       COSINE_DISTANCE(Vector, @query_vector) AS similarity
FROM Embeddings
WHERE Status = 'active'
ORDER BY similarity ASC
LIMIT 10;
```

The `ORDER BY` clause must use a distance function. The `LIMIT` specifies how many rows to return.

### With lexical filters

Combine distance metrics with `WHERE` predicates:

```sql
-- Find similar embeddings from a specific date range
SELECT Id, Content,
       L2_DISTANCE(Vector, @query_vector) AS distance
FROM Embeddings
WHERE CreatedAt >= '2026-03-01'
  AND CreatedAt <  '2026-04-01'
  AND Status = 'active'
ORDER BY distance ASC
LIMIT 5;
```

**Supported WHERE operators**:

- Comparison: `=`, `!=`, `<`, `>`, `<=`, `>=`
- Pattern: `LIKE`, `NOT LIKE`
- Null checks: `IS NULL`, `IS NOT NULL`
- Logical: `AND` (supported), `OR` (requires full table scan)

### With joins

Combine vector similarity with relational joins:

```sql
SELECT p.Name, p.Category, e.Score,
       COSINE_DISTANCE(e.Vector, @query_vector) AS similarity
FROM Embeddings e
INNER JOIN Products p ON e.ProductId = p.Id
WHERE p.Category = 'electronics'
  AND e.Status = 'active'
ORDER BY similarity ASC
LIMIT 20;
```

Query execution:

1. Use HNSW index (if available) to fetch candidate embeddings
2. Apply `WHERE` predicates on embedding table
3. Join filtered results with product table
4. Apply `ORDER BY distance` and `LIMIT`

## Approximate nearest-neighbor (ANN) search with HNSW

For large datasets, use HNSW (Hierarchical Navigable Small World) indexing for fast approximate results.

### Creating a vector index

```sql
CREATE VECTOR INDEX IX_Embeddings_Vector ON Embeddings(Vector) USING HNSW;
```

Configuration (optional):

```sql
CREATE VECTOR INDEX IX_Embeddings_Vector ON Embeddings(Vector)
USING HNSW
WITH (
    METRIC = 'cosine',
    M = 8,
    EF_CONSTRUCTION = 64,
    EF_SEARCH = 24
);
```

**Parameters**:

| Parameter         | Default | Range             | Effect                                                  |
| :---------------- | :------ | :---------------- | :------------------------------------------------------ |
| `METRIC`          | cosine  | cosine\|euclidean | Distance metric for index building                      |
| `M`               | 8       | 4–64              | Connectivity degree; higher = more connections          |
| `EF_CONSTRUCTION` | 64      | 32–256            | Search width during index build; higher = more accurate |
| `EF_SEARCH`       | 24      | 8–128             | Search width during query; higher = more accurate       |

### ANN query syntax

Use the same SQL syntax as exact search; the query planner automatically uses the index:

```sql
SELECT Id, Content,
       COSINE_DISTANCE(Vector, @query_vector) AS similarity
FROM Embeddings
WHERE Status = 'active'
ORDER BY similarity ASC
LIMIT 10;
```

_With a vector index on `Vector`, this query automatically uses HNSW instead of brute-force._

### Tuning ANN performance

**Accuracy vs. Speed tradeoff**:

```sql
-- Fast but approximate (low EF_SEARCH)
CREATE VECTOR INDEX IX_Fast ON Embeddings(Vector)
USING HNSW WITH (EF_SEARCH = 8);
-- Recall: ~60%, latency: 1ms

-- Balanced (default)
CREATE VECTOR INDEX IX_Balanced ON Embeddings(Vector)
USING HNSW WITH (EF_SEARCH = 24);
-- Recall: ~85%, latency: 5ms

-- Accurate but slower (high EF_SEARCH)
CREATE VECTOR INDEX IX_Accurate ON Embeddings(Vector)
USING HNSW WITH (EF_SEARCH = 128);
-- Recall: ~95%, latency: 20ms
```

Higher `EF_SEARCH` = better recall, higher latency.

## Hybrid queries: vector + joins + filters

DataVo's hybrid planner optimizes queries that combine vector search with lexical filters and joins.

### Example: Find similar products by category

```sql
SELECT p.Name, p.Price, e.Score,
       COSINE_DISTANCE(e.Vector, @query_vector) AS similarity
FROM ProductEmbeddings e
WHERE e.IsActive = 1
  AND e.CreatedAt > '2026-01-01'
INNER JOIN Products p ON e.ProductId = p.Id
WHERE p.Category = 'electronics'
ORDER BY similarity ASC
LIMIT 20;
```

**Execution flow**:

1. **Vector candidate fetch**: Use HNSW to get top candidates (e.g., top 100)
2. **Predicate filter**: Apply WHERE clauses on candidates
3. **Expand if needed**: If fewer than 20 rows remain, expand candidate set and retry
4. **Join**: Execute join on surviving candidates
5. **Final sort + limit**: Apply ORDER BY distance and return top 20

Benefits:

- Avoids full-table scan
- Short-circuits on lexical predicates
- Automatic expansion if needed

### Planner eligibility rules

The hybrid planner will use ANN + candidate first execution when:

1. Vector index exists on the column
2. `ORDER BY` uses distance function on indexed column
3. `WHERE` predicates reference only the embedding table (not joined tables)
4. No `OR` in WHERE clause (requires full evaluation)

If ineligible, the query falls back to brute-force evaluation.

## .NET / C# examples

### Using DataVo.Core directly

```csharp
using DataVo.Core;

using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Disk,
    DatabasePath = "./embeddings.db"
});

db.Execute("CREATE DATABASE Products");
db.Execute("USE Products");

db.Execute(@"
    CREATE TABLE Items (
        Id INT PRIMARY KEY,
        Name VARCHAR(100),
        Description VARCHAR(500),
        Vector VECTOR(384)
    )
");

// Insert an embedding
float[] embedding = new float[384] { /* ... */ };
db.ExecuteWithParams("INSERT INTO Items VALUES (@id, @name, @desc, @vec)",
    ("id", 1),
    ("name", "Widget"),
    ("desc", "A useful widget"),
    ("vec", embedding)
);

// Query with embedding vector
float[] queryVector = new float[384] { /* ... */ };
var results = db.ExecuteWithParams(@"
    SELECT Id, Name, COSINE_DISTANCE(Vector, @query) AS similarity
    FROM Items
    ORDER BY similarity ASC
    LIMIT 10
",
    ("query", queryVector)
);

foreach (var row in results)
{
    Console.WriteLine($"{row["Id"]}: {row["Name"]} (sim: {row["similarity"]})");
}
```

### Using Entity Framework

```csharp
using DataVo.Core;
using DataVo.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

public class AppDbContext : DataVoDbContext
{
    public DbSet<ItemEmbedding> Items { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseDataVo("./embeddings.db");
    }
}

public class ItemEmbedding
{
    public int Id { get; set; }
    public string Name { get; set; }
    public float[] Vector { get; set; } // VECTOR(768)
    public DateTime CreatedAt { get; set; }
}

// Query usage
using var db = new AppDbContext();

float[] queryVector = new float[768] { /* ... */ };

// Normal LINQ for non-vector expressions:
var names = db.Items.Where(x => x.Id > 0).Select(x => x.Name).ToList();

// LINQ vector expression translated in native preview:
var nearest = db.QueryFromDataVo<ItemEmbedding>(q => q
        .OrderBy(x => DataVoVectorDbFunctions.CosineDistance(EF.Functions, x.Vector, queryVector))
        .Take(10));
```

## Entity Framework — End-to-end example

This section provides a complete EF Core setup showing project init, `DbContext` configuration, schema creation, inserting vectors, and two query approaches:

- Native LINQ vector expressions via `DataVoVectorDbFunctions`
- Raw SQL via `FromSqlRaw` for advanced/custom SQL shapes

1. Project setup

```bash
dotnet new console -n VectorEfDemo
cd VectorEfDemo
dotnet add package DataVo.Core
dotnet add package DataVo.Data
dotnet add package DataVo.EntityFrameworkCore
```

2. Model and `DbContext`

Create `ItemEmbedding.cs`:

```csharp
public class ItemEmbedding
{
    public int Id { get; set; }
    public string Name { get; set; } = null!;
    public float[] Vector { get; set; } = null!; // maps to VECTOR(384)
    public DateTime CreatedAt { get; set; }
}
```

Create `AppDbContext.cs`:

```csharp
using DataVo.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

public class AppDbContext : DataVoDbContext
{
    public DbSet<ItemEmbedding> Items { get; set; } = null!;

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseDataVo("DataSource=./embeddings.db;StorageMode=Disk");

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<ItemEmbedding>(b =>
        {
            b.ToTable("Items");
            b.HasKey(e => e.Id);
            b.Property(e => e.Vector).HasColumnType("VECTOR(384)");
        });
    }
}
```

3. Program example (DDL, insert, query)

```csharp
using DataVo.Core;
using Microsoft.EntityFrameworkCore;

// Create schema and insert using DataVoContext (convenient for DDL + param binding)
using (var dv = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.Disk, DatabasePath = "./embeddings.db" }))
{
    dv.Execute("CREATE DATABASE IF NOT EXISTS Demo");
    dv.Execute("USE Demo");

    dv.Execute(@"
        CREATE TABLE IF NOT EXISTS Items (
            Id INT PRIMARY KEY,
            Name VARCHAR(100),
            Vector VECTOR(384),
            CreatedAt DATETIME
        )");

    dv.Execute("CREATE VECTOR INDEX IF NOT EXISTS IX_Items_Vector ON Items(Vector) USING HNSW");

    float[] sample = new float[384]; // fill with embedding
    dv.ExecuteWithParams("INSERT INTO Items VALUES (@id, @name, @vec, @now)",
        ("id", 1), ("name", "Alpha"), ("vec", sample), ("now", DateTime.UtcNow));
}

// EF usage
using (var ctx = new AppDbContext())
{
    // Ensure DataVo schema exists and mirror current rows into EF change tracker
    ctx.EnsureCreatedAndLoad();

    // A) LINQ vector query translated by DataVo native preview
    float[] q = new float[384]; // query embedding
    var nearest = ctx.QueryFromDataVo<ItemEmbedding>(s => s
        .Where(e => e.CreatedAt >= DateTime.UtcNow.AddMonths(-1))
        .OrderBy(e => DataVoVectorDbFunctions.CosineDistance(EF.Functions, e.Vector, q))
        .Take(10));

    // B) Guarded LINQ using QueryFromDataVo for simple non-vector shapes
    var recent = ctx.QueryFromDataVo<ItemEmbedding>(q => q
        .Where(e => e.CreatedAt >= DateTime.UtcNow.AddMonths(-1))
        .OrderBy(e => e.Id)
        .Take(20));

    Console.WriteLine($"Nearest count: {nearest.Count}, Recent count: {recent.Count}");
}
```

Guidance:

- Use `DataVoVectorDbFunctions.CosineDistance` with `EF.Functions` for LINQ vector queries in native translation preview.
- `DataVoVectorDbFunctions.L2Distance` is exposed as a typed API surface, but native LINQ translation for L2 is not enabled yet.
- Use `FromSqlRaw` for advanced/custom SQL surfaces, or when you need SQL-only syntax.
- `QueryFromDataVo` remains the guarded bridge entrypoint; it will run native translation when eligible and fall back safely otherwise.
- Map `float[]` -> `VECTOR(n)` with `.HasColumnType("VECTOR(n)")`.
- For production-sized datasets, create the HNSW index to enable ANN performance.

## Performance considerations

### Memory usage

- Each vector requires `dimension × 4` bytes (float32)
- 768-dimensional vector: ~3 KB
- 1M vectors: ~3 GB memory

### Index memory

HNSW index adds overhead:

- Metadata: ~50 bytes per vector
- Graph structure: ~200-400 bytes per vector (depends on `M`)
- 1M vectors with M=8: ~250 MB index overhead

### Disk space

- Table data: `(768 × 4) + metadata` bytes per row
- Index file: Persisted HNSW structure

### Query latency

| Operation         | Exact (brute-force) | HNSW (ANN) |
| :---------------- | :------------------ | :--------- |
| 1K rows, top-10   | 0.5 ms              | 0.1 ms     |
| 10K rows, top-10  | 5 ms                | 0.2 ms     |
| 100K rows, top-10 | 50 ms               | 0.3 ms     |
| 1M rows, top-10   | 500 ms              | 0.5 ms     |

_Latencies are approximate and depend on hardware and vector dimensionality._

## Troubleshooting

### Query returns no results

**Problem**: `ORDER BY distance` returns empty result set.

**Possible causes**:

1. Vector column is empty or NULL
2. Distance function references non-existent column
3. WHERE predicates too restrictive (no rows match)

**Solution**: Check data population and WHERE clause.

### Distance values seem wrong

**Problem**: Distance values are negative or unexpectedly high.

**Possible causes**:

1. Wrong distance metric for embeddings (COSINE vs L2)
2. Vectors not normalized (for COSINE metric)
3. Dimension mismatch between query and stored vectors

**Solution**: Verify embeddings match metric assumptions.

### Query is slow despite index

**Problem**: Queries are slower than expected even with HNSW index.

**Possible causes**:

1. Index not being used (WHERE predicates on joined table)
2. EF_SEARCH too low (high recall requirement)
3. Full table scan due to OR predicate in WHERE

**Solution**: Check query plan eligibility and adjust index parameters.

### Out-of-memory errors

**Problem**: High-dimensional embeddings or large datasets exhaust memory.

**Possible causes**:

1. Embeddings too large for in-memory index
2. Multiple vector indices consuming memory

**Solution**:

- Use disk-backed storage mode
- Reduce vector dimension if possible
- Consider filtering to smaller result sets

## Next steps

- [HNSW technical deep-dive](hnsw-and-hybrid-current-state.md)
- [Vector query roadmap](vector-db-hnsw-roadmap.md)
- [Schema and DDL reference](schema-and-ddl.md)
- [Query execution and planning](volcano-planner-and-execution.md)
