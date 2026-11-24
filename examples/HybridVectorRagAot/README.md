# Hybrid Vector RAG, as one Native AOT binary

Local retrieval for a RAG pipeline with DataVo: document chunks with SQL metadata (`Category`, `CreatedAt DATETIME`)
and `VECTOR(384)` embeddings in one table, an HNSW index, and SQL that filters on metadata while it ranks by cosine
distance. It publishes as **a single native executable** with no `.dll`, `.so` or `.dylib` beside it and no runtime
to install.

```sql
SELECT Id, Category, CreatedAt, Text, Embedding <=> @q AS Distance
FROM chunks
WHERE Category = @category
  AND CreatedAt >= @since
  AND Embedding <=> @q < 0.95
ORDER BY Distance ASC
LIMIT 5
```

## Run it

```bash
dotnet run -c Release                                   # built-in demo question
dotnet run -c Release -- "automatic key rotation policy"
dotnet run -c Release -- --data ./rag-data              # keep the database on disk (LSM engine) between runs
```

## Publish the single binary

```bash
dotnet publish -c Release -r osx-arm64 -o publish       # or linux-x64, linux-arm64, win-x64
./publish/HybridVectorRagAot "automatic key rotation policy"
```

The publish folder holds one executable (about 14 MB on macOS arm64), plus a `.dSYM` folder of stripped debug
symbols that is not needed to run it. `otool -L publish/HybridVectorRagAot` (or `ldd` on Linux) lists only
operating-system libraries. The publish produces no trim or AOT warnings: `DataVo.Core` is AOT-clean by construction
(its trim/AOT analyzer diagnostics are build errors).

## What it shows

1. **Semantic top-5.** Pure nearest-neighbour search. For "how often should we rotate encryption keys" the winner is
   a stale January 2025 runbook ("every ninety days").
2. **Hybrid search.** The same ranking with `Category = 'security' AND CreatedAt >= 2026-01-01` and a distance
   threshold. The stale runbook is filtered out and the current 2026 policy ("every thirty days") comes first. This
   is the point of hybrid retrieval: structured filters keep outdated context out of the model's prompt.
3. **`EXPLAIN`.** The query runs as `VectorHnswScan` (an HNSW fast path), with the hybrid router's decisions in
   `Details`.
4. **Latency.** Warm hybrid-query latency, about 80 µs per query under Native AOT and about 300 µs on the JIT
   (Apple M4 Pro, 31 chunks).

## The embedder

`HashingEmbedder` is a dependency-free stand-in for a sentence-embedding model: feature hashing of words, word pairs
and character trigrams into 384 signed buckets, normalized to unit length. It captures lexical similarity, which is
enough to show the retrieval pipeline without downloading a model. In a real application replace `Embed` with your
model's output. 384 dimensions matches all-MiniLM-L6-v2 and bge-small; for another size, change `VECTOR(384)`.

## Files

| File | Purpose |
| --- | --- |
| `Program.cs` | Schema, load, indexes, the three queries, `EXPLAIN`, latency. |
| `Corpus.cs` | The built-in knowledge base (11 documents, 31 chunks, created 2025 to 2026). |
| `HashingEmbedder.cs` | The stand-in embedding model. |
