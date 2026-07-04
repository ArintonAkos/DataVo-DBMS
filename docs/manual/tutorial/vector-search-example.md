# Vector Search Example

This tutorial uses `DataVoContext` to create a tiny vector dataset and run nearest-neighbor queries. The example uses three-dimensional vectors so the data is readable, but the SQL shape is the same for production embeddings such as `VECTOR(1536)`.

Create an in-memory context and select a database.

```csharp
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory
});

db.Execute("CREATE DATABASE Vectors");
db.Execute("USE Vectors");
```

Create a table with a vector column.

```csharp
db.Execute("""
CREATE TABLE Embeddings (
  Id INT PRIMARY KEY,
  Label VARCHAR(80),
  Emb VECTOR(3)
);
""");
```

Insert a few vectors. DataVo accepts bracketed numeric vector literals.

```csharp
db.Execute("INSERT INTO Embeddings (Id, Label, Emb) VALUES (1, 'red', '[1,0,0]')");
db.Execute("INSERT INTO Embeddings (Id, Label, Emb) VALUES (2, 'green', '[0,1,0]')");
db.Execute("INSERT INTO Embeddings (Id, Label, Emb) VALUES (3, 'warm-red', '[0.9,0.1,0]')");
```

Create a vector index. `FLAT` is exact and easy to reason about for small datasets.

```csharp
db.Execute("CREATE INDEX idx_embeddings_flat ON Embeddings (Emb) USING FLAT");
```

Run a cosine-distance query. Lower distance values are closer.

```csharp
QueryResult cosine = db.Execute("""
SELECT Id, Label, Emb <=> '[1,0,0]' AS distance
FROM Embeddings
ORDER BY distance ASC
LIMIT 3;
""")[0];

foreach (Dictionary<string, object?> row in cosine.Data)
{
    Console.WriteLine($"{row["Label"]}: {row["distance"]}");
}
```

Expected output:

```text
red: 0
warm-red: 0.006116...
green: 1
```

Run the same query with L2 distance when Euclidean distance is the right measure for the embedding space.

```csharp
QueryResult l2 = db.Execute("""
SELECT Id, Label, Emb <-> '[1,0,0]' AS distance
FROM Embeddings
ORDER BY distance ASC
LIMIT 3;
""")[0];
```

For larger tables, create an HNSW index instead. HNSW is approximate, but it is designed to avoid scanning every vector. DataVo v0.1 uses built-in HNSW defaults; it does not expose tuning parameters such as `efConstruction` or `M`.

```csharp
db.Execute("CREATE INDEX idx_embeddings_hnsw ON Embeddings (Emb) USING HNSW");
```

Use scalar filters and vector ranking together when the application needs metadata-aware search.

```csharp
QueryResult filtered = db.Execute("""
SELECT Id, Label, Emb <=> '[1,0,0]' AS distance
FROM Embeddings
WHERE Label LIKE 'warm%'
ORDER BY distance ASC
LIMIT 10;
""")[0];
```
