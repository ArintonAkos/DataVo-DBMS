---
title: Microsoft.Extensions.VectorData (DataVo.VectorData)
description: Use DataVo as a Microsoft.Extensions.VectorData vector store with Microsoft.Extensions.AI embeddings, HNSW/FLAT indexes, LINQ filters and in-memory or LSM persistence.
---

# Microsoft.Extensions.VectorData

`DataVo.VectorData` implements the standard .NET vector store abstractions, [`Microsoft.Extensions.VectorData`](https://learn.microsoft.com/dotnet/ai/vector-stores/overview), on top of DataVo. Code written against `VectorStore` and `VectorStoreCollection<TKey, TRecord>` (Semantic Kernel, Microsoft.Extensions.AI samples, your own RAG pipeline) runs on an embedded DataVo database: in memory, or persisted by the LSM engine, in one process, with no server.

Each collection is an ordinary DataVo table. The key is its primary key, each data property a column, and each vector property a `VECTOR(n)` column with an `HNSW` or `FLAT` index. The same rows stay queryable from SQL, `EXPLAIN` and reactive views.

```bash
dotnet add package DataVo.VectorData --prerelease
```

The package targets `net8.0`, `net9.0` and `net10.0`, and builds with every trim and Native AOT diagnostic treated as an error.

## Quickstart

```csharp
using DataVo.Core.StorageEngine.Config;
using DataVo.VectorData;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.VectorData;

public sealed class Doc
{
    [VectorStoreKey]
    public Guid Id { get; set; }

    [VectorStoreData(IsIndexed = true)]
    public string Category { get; set; } = "";

    [VectorStoreData]
    public string Text { get; set; } = "";

    [VectorStoreData(IsIndexed = true)]
    public DateTime UpdatedAt { get; set; }

    [VectorStoreVector(384, DistanceFunction = DistanceFunction.CosineSimilarity, IndexKind = IndexKind.Hnsw)]
    public ReadOnlyMemory<float> Embedding { get; set; }
}

IEmbeddingGenerator<string, Embedding<float>> generator = /* any Microsoft.Extensions.AI generator */;

using var store = new DataVoVectorStore(new DataVoConfig { StorageMode = StorageMode.Lsm, DataDirectory = "./vectors" });
var docs = new DataVoVectorStoreCollection<Guid, Doc>(store, "docs");
await docs.EnsureCollectionExistsAsync();

Embedding<float> embedding = (await generator.GenerateAsync(["DataVo stores vectors next to rows."]))[0];
await docs.UpsertAsync(new Doc { Category = "guide", Text = "...", UpdatedAt = DateTime.UtcNow, Embedding = embedding.Vector });

ReadOnlyMemory<float> query = (await generator.GenerateAsync(["where are vectors stored?"]))[0].Vector;
DateTime since = DateTime.UtcNow.AddDays(-30);
await foreach (VectorSearchResult<Doc> hit in docs.SearchAsync(query, top: 5, new VectorSearchOptions<Doc>
{
    Filter = doc => doc.Category == "guide" && doc.UpdatedAt >= since,
}))
{
    Console.WriteLine($"{hit.Score:F3} {hit.Record.Text}");
}
```

`Embedding<float>.Vector` is a `ReadOnlyMemory<float>`, so embeddings flow from the generator into the record and into the statement parameter without a `ToArray()` copy. A `Guid` key left at `Guid.Empty` is generated on upsert and written back to the record.

## Schema Mapping

| Attribute / definition property | DataVo column | Index |
| --- | --- | --- |
| `[VectorStoreKey]` on `int`, `long`, `Guid` or `string` | `INT`, `BIGINT`, `GUID`, `VARCHAR` `PRIMARY KEY` | Primary key |
| `[VectorStoreData]` | `VARCHAR`, `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `BIT`, `GUID`, `DATETIME` (`DateTime`, `DateTimeOffset`), `DATE` (`DateOnly`); `string[]` / `List<string>` as JSON text | `IsIndexed = true` adds `CREATE INDEX ... USING BTREE` |
| `[VectorStoreVector(n)]` on `ReadOnlyMemory<float>`, `ReadOnlyMemory<float>?`, `float[]` or `Embedding<float>` | `VECTOR(n)` | `IndexKind.Hnsw` (default) or `IndexKind.Flat` |
| `[VectorStoreVector(n)]` on `string` | `VECTOR(n)` filled by the embedding generator | as above |

A `VectorStoreCollectionDefinition` passed to the collection replaces the attributes (useful when the dimension is only known at run time). `StorageName` renames a column. DataVo SQL has no quoted identifiers, so a property whose name is a reserved word gets a trailing underscore: a property named `Vector` is stored in column `Vector_`.

| `DistanceFunction` | DataVo operator / index metric | Score reported |
| --- | --- | --- |
| `CosineDistance` (default) | `<=>`, `cosine` | distance (smaller is closer) |
| `CosineSimilarity` | `<=>`, `cosine` | `1 - distance` (larger is closer) |
| `EuclideanDistance` | `<->`, `euclidean` | distance |
| `EuclideanSquaredDistance` | `<->`, `euclidean` | distance² |

Other distance functions (dot product, Manhattan, Hamming) and index kinds (IVF, DiskANN) throw `NotSupportedException` when the collection is created.

## Operations

| Method | DataVo behaviour |
| --- | --- |
| `EnsureCollectionExistsAsync` / `CollectionExistsAsync` / `EnsureCollectionDeletedAsync` | `CREATE TABLE` plus indexes, catalog lookup, `DROP TABLE` |
| `UpsertAsync(record)` / `UpsertAsync(records)` | One transaction per call: delete-then-insert by key, with all embeddings generated first in one batch |
| `GetAsync(key)` / `GetAsync(keys)` | Primary-key lookup; `IN (...)` in batches of 256 keys |
| `GetAsync(filter, top, options)` | `WHERE` from the LINQ filter, `ORDER BY` from `options.OrderBy`, `LIMIT`/`OFFSET` |
| `DeleteAsync(key)` / `DeleteAsync(keys)` | One transaction per call |
| `SearchAsync(value, top, options)` | `ORDER BY vector <op> @query LIMIT top OFFSET skip`, with `Filter` as `WHERE` and `ScoreThreshold` as a distance bound |

Vectors are only read back when `IncludeVectors = true`. Search inputs may be `ReadOnlyMemory<float>`, `float[]` or `Embedding<float>`, or a `string` when an embedding generator is configured.

## Filters

`Filter` expressions are translated to DataVo SQL with every value bound as a parameter. Captured variables and constants are evaluated without compiling the expression, so translation is Native AOT safe.

| C# | SQL |
| --- | --- |
| `==`, `!=`, `<`, `<=`, `>`, `>=` against a value (either side) | `col = @p`, ... |
| `x == null`, `x != null` | `col IS NULL`, `col IS NOT NULL` |
| `&&`, `\|\|`, `!` | `AND`, `OR`, `NOT (...)` |
| `doc.Published` (a `bool` property) | `Published = TRUE` |
| `ids.Contains(doc.Id)` (arrays, lists, C# 14 span overloads) | `Id IN (@p0, @p1, ...)` |
| `doc.Path.StartsWith(s)`, `EndsWith(s)`, `Contains(s)` | `LIKE` patterns |

DataVo `LIKE` has no escape character: a `StartsWith` value containing `%` or `_` is translated to an exact ordinal range instead, and `EndsWith`/`Contains` with those characters throw `NotSupportedException`. Vector properties and string-list properties cannot be filtered. Anything else throws `NotSupportedException` with the untranslatable expression.

## Embedding Generation

Set `DataVoVectorStoreOptions.EmbeddingGenerator` (or `VectorStoreCollectionDefinition.EmbeddingGenerator`, or a vector property's own generator) to an `IEmbeddingGenerator<string, Embedding<float>>`. A `string` vector property is then embedded on upsert, and `SearchAsync("text", ...)` embeds the query. The record keeps the text: `IncludeVectors = true` is rejected for such properties.

```csharp
var store = new DataVoVectorStore(config, new DataVoVectorStoreOptions { EmbeddingGenerator = generator });
```

## Dependency Injection

```csharp
services.AddSingleton<IEmbeddingGenerator<string, Embedding<float>>>(generator);
services.AddDataVoVectorStore(new DataVoConfig { StorageMode = StorageMode.Lsm, DataDirectory = "./vectors" });
services.AddDataVoCollection<Guid, Doc>("docs");

// Inject VectorStore, DataVoVectorStore, VectorStoreCollection<Guid, Doc> or IVectorSearchable<Doc>.
```

`AddDataVoVectorStore()` without a config uses a `DataVoContext` already registered in the container; `AddKeyedDataVoVectorStore(key, config)` registers keyed stores. A registered `IEmbeddingGenerator<string, Embedding<float>>` becomes the store's generator unless the options set one.

## Native AOT and Trimming

- `new DataVoVectorStoreCollection<TKey, TRecord>(store, name)` and `AddDataVoCollection<TKey, TRecord>` are trim and AOT safe: `TRecord`'s public properties and parameterless constructor are preserved through `[DynamicallyAccessedMembers]`.
- `VectorStore.GetCollection<TKey, TRecord>` is annotated `[RequiresUnreferencedCode]` / `[RequiresDynamicCode]` by `Microsoft.Extensions.VectorData` itself; `DataVoVectorStore` keeps that contract. Prefer the constructor in AOT apps.
- `GetDynamicCollection(name, definition)` uses `Dictionary<string, object?>` records and needs no reflection.

## Dynamic Collections

```csharp
var definition = new VectorStoreCollectionDefinition
{
    Properties =
    [
        new VectorStoreKeyProperty("Id", typeof(int)),
        new VectorStoreDataProperty("Category", typeof(string)) { IsIndexed = true },
        new VectorStoreVectorProperty("Embedding", typeof(ReadOnlyMemory<float>), 384) { IndexKind = IndexKind.Flat },
    ],
};
var items = store.GetDynamicCollection("items", definition);
var hits = items.SearchAsync(query, top: 5, new VectorSearchOptions<Dictionary<string, object?>>
{
    Filter = record => (string)record["Category"]! == "guide",
});
```

## SQL Access to the Same Data

The store's tables live in the database named by `DataVoVectorStoreOptions.DatabaseName` (default `VectorStore`), so the same data answers SQL and `EXPLAIN`:

```csharp
var rows = store.Context.Execute("""
    USE VectorStore;
    EXPLAIN SELECT Id, Embedding <=> '[...]' AS distance FROM docs
    WHERE Category = 'guide' ORDER BY distance LIMIT 5
    """);
```

## Example: Local RAG over the DataVo Manual

[`examples/MicrosoftAiLocalRag`](https://github.com/ArintonAkos/DataVo-DBMS/tree/master/examples/MicrosoftAiLocalRag) indexes every section of this manual with `IEmbeddingGenerator<string, Embedding<float>>`, stores them in a `DataVoVectorStore` on the LSM engine with `Category`, `FilePath` and `UpdatedAt` (`DATETIME`) metadata, and runs semantic and filtered hybrid searches followed by `EXPLAIN`. It runs offline with a built-in deterministic generator:

```bash
dotnet run -c Release --project examples/MicrosoftAiLocalRag
dotnet run -c Release --project examples/MicrosoftAiLocalRag -- --ollama all-minilm
```

## See Also

- [Vector Search](/manual/fundamentals/vector-search): `VECTOR(n)` columns, distance operators, HNSW/FLAT tuning and filtered search in SQL
- [Native AOT](/manual/client-interfaces/native-aot): publishing DataVo applications as native binaries
- [LSM & Durability](/manual/fundamentals/lsm-and-durability): how vector indexes recover on reopen
