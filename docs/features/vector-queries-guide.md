# Vector Queries Guide

> Last updated: 2026-03-24
> Status: Active feature with HNSW support
> Audience: Application developers using vector/embedding queries

## Overview

DataVo provides first-class vector search capabilities for similarity-based queries on embedding data. This guide covers:

- Creating and querying vector columns
- Distance metrics and similarity operators
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
    Vector VECTOR(3),
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
    (1, 'Hello world', '[1,0,0]', '2026-03-20', 'active');

INSERT INTO Embeddings VALUES
    (2, 'Another embedding', '[0,1,0]', '2026-03-21', 'active');
```

Constraints:

- All components must be finite (no `NaN` or `Infinity`)
- Dimension must match column definition exactly
- JSON array format: `[float, float, ..., float]`

### Invalid vectors (rejected)

```sql
-- ERROR: Contains NaN
INSERT INTO Embeddings VALUES (1, 'text', '[0.1,NaN,0.3]', '2026-03-20', 'active');

-- ERROR: Dimension mismatch (3 expected, 2 provided)
INSERT INTO Embeddings VALUES (1, 'text', '[0.1,0.2]', '2026-03-20', 'active');

-- ERROR: Non-finite value
INSERT INTO Embeddings VALUES (1, 'text', '[0.1,Infinity,0.3]', '2026-03-20', 'active');
```

## Distance metrics and similarity operators

DataVo supports two common distance metrics for nearest-neighbor search:

### Euclidean distance (L2)

Straight-line distance in vector space. Use when:

- Embeddings are normalized and magnitude matters
- You want scale-sensitive similarity

```sql
SELECT Id, Content,
       Vector <-> '[1,0,0]' AS distance
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
       Vector <=> '[1,0,0]' AS distance
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
SELECT Id, Content,
       Vector <=> '[1,0,0]' AS similarity
FROM Embeddings
WHERE Status = 'active'
ORDER BY similarity ASC
LIMIT 10;
```

The `ORDER BY` clause must use a distance operator expression. The `LIMIT` specifies how many rows to return.

### With lexical filters

Combine distance metrics with `WHERE` predicates:

```sql
-- Find similar embeddings from a specific date range
SELECT Id, Content,
       Vector <-> '[1,0,0]' AS distance
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
SELECT p.Name, p.Category,
       e.Vector <=> '[1,0,0]' AS similarity
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
CREATE INDEX IX_Embeddings_Vector ON Embeddings (Vector) USING HNSW;
```

The current public SQL surface uses `CREATE INDEX ... USING HNSW`; index tuning options are part of ongoing hardening work.

### ANN query syntax

Use the same SQL syntax as exact search; the query planner automatically uses the index:

```sql
SELECT Id, Content,
       Vector <=> '[1,0,0]' AS similarity
FROM Embeddings
WHERE Status = 'active'
ORDER BY similarity ASC
LIMIT 10;
```

_With a vector index on `Vector`, this query automatically uses HNSW instead of brute-force._

### Tuning ANN performance

**Accuracy vs. Speed tradeoff**:

Index tuning controls are not part of the documented public SQL syntax yet. Validate recall and latency on representative data while the HNSW surface continues to harden.

## Hybrid queries: vector + joins + filters

DataVo's hybrid planner optimizes queries that combine vector search with lexical filters and joins.

### Example: Find similar products by category

```sql
SELECT p.Name, p.Price,
       e.Vector <=> '[1,0,0]' AS similarity
FROM ProductEmbeddings e
INNER JOIN Products p ON e.ProductId = p.Id
WHERE p.Category = 'electronics'
  AND e.IsActive = 1
  AND e.CreatedAt > '2026-01-01'
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
2. `ORDER BY` uses a distance operator expression on the indexed column
3. `WHERE` predicates reference only the embedding table (not joined tables)
4. No `OR` in WHERE clause (requires full evaluation)

If ineligible, the query falls back to brute-force evaluation.

## .NET / C# examples

### Using DataVo.Data with ADO.NET

```csharp
using DataVo.Data;

using var connection = new DataVoConnection("StorageMode=Disk;DataSource=Products");
connection.Open();

using var create = connection.CreateCommand();
create.CommandText = @"
    CREATE TABLE Items (
        Id INT PRIMARY KEY,
        Name VARCHAR(100),
        Description VARCHAR(500),
        Vector VECTOR(3)
    )";
create.ExecuteNonQuery();

// Insert an embedding.
// Vector values are currently passed as SQL vector literal strings in ADO.NET examples.
string embedding = "[0.1,0.2,0.3]";
using var insert = connection.CreateCommand();
insert.CommandText = "INSERT INTO Items VALUES (@id, @name, @desc, @vec)";
insert.Parameters.AddWithValue("@id", 1);
insert.Parameters.AddWithValue("@name", "Widget");
insert.Parameters.AddWithValue("@desc", "A useful widget");
insert.Parameters.AddWithValue("@vec", embedding);
insert.ExecuteNonQuery();

// Query with embedding vector
string queryVector = "[0.2,0.1,0.4]";
using var query = connection.CreateCommand();
query.CommandText = @"
    SELECT Id, Name, Vector <=> @query AS similarity
    FROM Items
    ORDER BY similarity ASC
    LIMIT 10
";
query.Parameters.AddWithValue("@query", queryVector);

using var results = query.ExecuteReader();
while (results.Read())
{
    Console.WriteLine($"{results["Id"]}: {results["Name"]} (sim: {results["similarity"]})");
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
    public float[] Vector { get; set; } // VECTOR(3)
    public DateTime CreatedAt { get; set; }
}

// Query usage
using var db = new AppDbContext();

float[] queryVector = new float[] { 1f, 0f, 0f };

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
- Raw SQL through `ExecuteSqlOnDataVo`, `ExecuteDataVoSqlRaw`, or ADO.NET `DataVoConnection` for advanced/custom SQL shapes

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
    public float[] Vector { get; set; } = null!; // maps to VECTOR(3)
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
            b.Property(e => e.Vector).HasColumnType("VECTOR(3)");
        });
    }
}
```

3. Program example (DDL, insert, query)

```csharp
using DataVo.Data;
using Microsoft.EntityFrameworkCore;

// Create schema and insert using the ADO.NET package.
using (var connection = new DataVoConnection("StorageMode=Disk;DataSource=VectorDemo"))
{
    connection.Open();

    using var create = connection.CreateCommand();
    create.CommandText = @"
        CREATE TABLE IF NOT EXISTS Items (
            Id INT PRIMARY KEY,
            Name VARCHAR(100),
            Vector VECTOR(3),
            CreatedAt DATETIME
        )";
    create.ExecuteNonQuery();

    using var index = connection.CreateCommand();
    index.CommandText = "CREATE INDEX IX_Items_Vector ON Items (Vector) USING HNSW";
    index.ExecuteNonQuery();

    // Vector values are currently passed as SQL vector literal strings in ADO.NET examples.
    string sample = "[0.1,0.2,0.3]";
    using var insert = connection.CreateCommand();
    insert.CommandText = "INSERT INTO Items VALUES (@id, @name, @vec, @now)";
    insert.Parameters.AddWithValue("@id", 1);
    insert.Parameters.AddWithValue("@name", "Alpha");
    insert.Parameters.AddWithValue("@vec", sample);
    insert.Parameters.AddWithValue("@now", DateTime.UtcNow);
    insert.ExecuteNonQuery();
}

// EF usage
using (var ctx = new AppDbContext())
{
    // Ensure DataVo schema exists and mirror current rows into EF change tracker
    ctx.EnsureCreatedAndLoad();

    // A) LINQ vector query translated by DataVo native preview
    float[] q = new float[] { 1f, 0f, 0f };
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

- Use `DataVoVectorDbFunctions.CosineDistance` with `EF.Functions` for LINQ vector queries; native translation emits the `<=>` cosine operator.
- Use `DataVoVectorDbFunctions.L2Distance` with `EF.Functions` for L2 vector queries; native translation emits the `<->` L2 operator.
- Use `ExecuteSqlOnDataVo`, `ExecuteDataVoSqlRaw`, or ADO.NET `DataVoConnection` for advanced/custom SQL surfaces, or when you need SQL-only syntax.
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
2. ANN recall/latency tradeoff needs validation on representative data
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
