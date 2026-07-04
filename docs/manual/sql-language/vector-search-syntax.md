# Vector Search and Indexes

DataVo stores embeddings in `VECTOR(n)` columns, where `n` is the fixed number of floating-point dimensions in the vector. Every row in that column must use the same dimension. If your model emits 1536-dimensional embeddings, declare `VECTOR(1536)`. If you are learning with small examples, `VECTOR(3)` is easier to read.

Start by creating a table with an integer primary key, a label, and a vector column.

```sql
CREATE TABLE Embeddings (
  Id INT PRIMARY KEY,
  Label VARCHAR(80),
  Emb VECTOR(3)
);
```

Insert vectors as bracketed numeric literals. The examples below use three dimensions so the values are easy to inspect by eye.

```sql
INSERT INTO Embeddings (Id, Label, Emb)
VALUES (1, 'red', '[1,0,0]');

INSERT INTO Embeddings (Id, Label, Emb)
VALUES (2, 'green', '[0,1,0]');

INSERT INTO Embeddings (Id, Label, Emb)
VALUES (3, 'blue', '[0,0,1]');

INSERT INTO Embeddings (Id, Label, Emb)
VALUES (4, 'warm-red', '[0.9,0.1,0]');
```

To search vectors efficiently, create a vector index on the vector column. DataVo supports two index families in v0.1 Alpha: `HNSW` and `FLAT`.

Use `FLAT` when you want perfect accuracy and the dataset is small enough that scanning every vector is acceptable. A flat index is simple: compare the query vector with every stored vector and return the exact nearest rows. It is a good default for tests, small local datasets, and correctness checks.

```sql
CREATE INDEX idx_embeddings_flat
ON Embeddings (Emb)
USING FLAT;
```

Use `HNSW` when the dataset is large and query speed matters more than exact exhaustive search. HNSW builds an approximate nearest-neighbor graph so DataVo can avoid comparing against every vector. Use it only after measuring recall, build time, and query latency on your workload. v0.1 does not currently expose HNSW tuning parameters.

```sql
CREATE INDEX idx_embeddings_hnsw
ON Embeddings (Emb)
USING HNSW;
```

Once the data and index exist, write vector queries with distance operators. DataVo ranks nearer rows by smaller distance values, so nearest-neighbor queries normally sort ascending and use `LIMIT`.

Cosine distance uses the `<=>` operator. It is commonly used for text embeddings because it compares direction more than magnitude.

```sql
SELECT Id, Label, Emb <=> '[1,0,0]' AS distance
FROM Embeddings
ORDER BY distance ASC
LIMIT 3;
```

L2 distance uses the `<->` operator. It measures Euclidean distance, which is useful when absolute geometric distance matters for your embedding space.

```sql
SELECT Id, Label, Emb <-> '[1,0,0]' AS distance
FROM Embeddings
ORDER BY distance ASC
LIMIT 3;
```

You can combine vector ranking with ordinary scalar filters. This is the common shape for application search: restrict the candidate set by metadata, then rank the remaining rows by vector distance.

```sql
SELECT Id, Label, Emb <=> '[1,0,0]' AS distance
FROM Embeddings
WHERE Label LIKE 'warm%'
ORDER BY distance ASC
LIMIT 10;
```

You can also use a vector distance predicate directly in `WHERE`. This returns only rows closer than the threshold and then orders the survivors by the same distance expression.

```sql
SELECT Id, Label, Emb <=> '[1,0,0]' AS distance
FROM Embeddings
WHERE Emb <=> '[1,0,0]' < 0.25
ORDER BY distance ASC
LIMIT 10;
```

For production-sized embeddings, the same SQL shape works with larger dimensions. The only difference is the column declaration and the vector literal length.

```sql
CREATE TABLE Documents (
  Id INT PRIMARY KEY,
  Title VARCHAR(200),
  ContentEmbedding VECTOR(1536)
);

CREATE INDEX idx_documents_embedding
ON Documents (ContentEmbedding)
USING HNSW;
```

Pick `FLAT` first when you are validating correctness. Move to `HNSW` when the table grows large enough that exact scans are too slow for your latency target.
