# Microsoft.Extensions.AI local RAG over the DataVo manual

Retrieval for a RAG pipeline built only on the standard .NET AI abstractions:

- **`IEmbeddingGenerator<string, Embedding<float>>`** (Microsoft.Extensions.AI) turns manual sections and questions into embeddings.
- **`VectorStoreCollection<string, ManualChunk>`** (Microsoft.Extensions.VectorData), implemented by **`DataVo.VectorData`**, stores them in an embedded DataVo database (LSM on disk by default) and searches them.

The demo indexes every `##` section of `docs/manual/**/*.md` with `Category`, `FilePath` and `UpdatedAt` (a DataVo `DATETIME`) metadata, then for each question runs:

1. a semantic search (`SearchAsync` over an HNSW cosine index);
2. a hybrid search: the same query with `Filter = chunk => chunk.Category == "fundamentals" && chunk.UpdatedAt >= since`, translated to a SQL `WHERE`;
3. `EXPLAIN` of the equivalent SQL, because the collection is an ordinary DataVo table;
4. a reopen of the LSM directory and one more search, without re-embedding.

## Run

```bash
# Offline: a built-in deterministic hashing embedder, no model, no network, no API key
dotnet run -c Release --project examples/MicrosoftAiLocalRag

# Your own question, kept in memory instead of on disk
dotnet run -c Release --project examples/MicrosoftAiLocalRag -- --memory --question "How are HNSW snapshots recovered?"

# Real embeddings from a local Ollama (ollama pull all-minilm)
dotnet run -c Release --project examples/MicrosoftAiLocalRag -- --ollama all-minilm
dotnet run -c Release --project examples/MicrosoftAiLocalRag -- --ollama nomic-embed-text --ollama-url http://localhost:11434
```

The demo is Native AOT compatible: `dotnet publish -c Release -r <rid>` (for example `osx-arm64`, `linux-x64`, `win-x64`) produces a single ~13 MB native executable that runs the same pipeline.

The offline generator (`HashingEmbeddingGenerator`) hashes words and word pairs into 384 buckets: it finds sections that share vocabulary with the question, not paraphrases. Any other `IEmbeddingGenerator<string, Embedding<float>>` (Ollama, OpenAI, Azure OpenAI, ONNX) plugs in unchanged; the collection's vector size follows the generator.

`OllamaEmbeddingGenerator` is a small dependency-free client for Ollama's `/api/embed` endpoint with source-generated JSON.

## Files

| File | What it shows |
| --- | --- |
| `Program.cs` | Batch embedding with `GenerateAsync`, upsert, semantic and filtered search, `EXPLAIN`, LSM reopen |
| `ManualChunk.cs` | The record, a `VectorStoreCollectionDefinition` built from the generator's dimension, and the markdown splitter |
| `HashingEmbeddingGenerator.cs` | The offline deterministic `IEmbeddingGenerator` |
| `OllamaEmbeddingGenerator.cs` | The optional local Ollama `IEmbeddingGenerator` |

See [Microsoft.Extensions.VectorData](../../docs/manual/client-interfaces/vector-data.md) in the manual for the full mapping, filter and DI reference.
