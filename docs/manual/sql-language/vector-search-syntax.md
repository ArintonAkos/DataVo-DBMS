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

To search vectors efficiently, create a vector index on the vector column. DataVo supports two index families in v0.2: `HNSW` and `FLAT`.

Use `FLAT` when you want perfect accuracy and the dataset is small enough that scanning every vector is acceptable. A flat index is simple: compare the query vector with every stored vector and return the exact nearest rows. It is a good default for tests, small local datasets, and correctness checks.

```sql
CREATE INDEX idx_embeddings_flat
ON Embeddings (Emb)
USING FLAT;
```

Use `HNSW` when the dataset is large and query speed matters more than exact exhaustive search. HNSW builds an approximate nearest-neighbor graph so DataVo can avoid comparing against every vector. Use it only after measuring recall, build time, and query latency on your workload.

```sql
CREATE INDEX idx_embeddings_hnsw
ON Embeddings (Emb)
USING HNSW;
```

`WITH (...)` sets the metric and the graph parameters. Every option is optional; an option you leave out keeps its default. Option names ignore case and underscores (`ef_construction` and `EfConstruction` are the same option).

```sql
CREATE INDEX idx_embeddings_tuned
ON Embeddings (Emb)
USING HNSW WITH (metric = 'cosine', m = 24, ef_construction = 128, ef_search = 96, enable_diversity_heuristic = true);
```

| Option | Default | Range | What it does |
|---|---|---|---|
| `metric` | `cosine` | `cosine`, `euclidean` (or `l2`) | The distance the index ranks by. Also accepted by `FLAT`. |
| `m` | `16` | 2 to 128 | Links each vector keeps per graph layer (twice as many on the bottom layer). More links raise recall, memory, and build time. |
| `ef_construction` | `64` | 1 to 4096 | Candidates examined when a vector is inserted. Higher values build a better graph more slowly. |
| `ef_search` | `64` | 1 to 4096 | The smallest candidate list of a search. Higher values raise recall and latency. |
| `enable_diversity_heuristic` | `true` | `true`, `false` | Prefers neighbors that are not close to one another, which keeps links between clusters. Leave it on for clustered data such as text embeddings; turning it off builds faster and can lose recall across clusters. |

The options are saved in the catalog with the index. When the graph has to be rebuilt from the table's rows, for example after its saved copy is lost, it is rebuilt with the same options. A value outside its range, an unknown option, or a graph option on a `FLAT` or B-tree index is an error and creates no index.

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

A filtered search still returns the exact number of rows the `LIMIT` asks for when that many rows pass the filter. DataVo asks the index for more candidates than the `LIMIT` (about `LIMIT / selectivity`), tests the filter on each candidate's stored row, and builds only the rows that pass. Two cost rules keep selective filters fast:

- When the filter needs so many candidates that walking the HNSW graph would cost more than comparing every stored vector, the HNSW index scans its vectors instead. That scan is exact.
- When even the widest candidate search finds too few passing rows, DataVo finds the passing rows first with a table scan and ranks only those by distance. That answer is exact as well.

The same applies to a filtered search over a join, such as `FROM Docs d JOIN Authors a ON d.AuthorId = a.Id WHERE a.Country = 'DE'`: the candidates are joined and filtered in stored form, and a joined table is read by primary key only for the candidates that refer to it.

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
