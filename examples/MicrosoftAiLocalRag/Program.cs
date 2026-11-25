using System.Diagnostics;
using System.Globalization;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;
using DataVo.VectorData;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.VectorData;
using MicrosoftAiLocalRag;

// Local RAG retrieval over DataVo's own manual with the standard .NET AI abstractions:
//   IEmbeddingGenerator<string, Embedding<float>>  (Microsoft.Extensions.AI)       -> embeddings
//   VectorStoreCollection<string, ManualChunk>      (Microsoft.Extensions.VectorData) -> storage and search, by DataVo.VectorData
//
// Usage: dotnet run -c Release [-- --ollama [model]] [--ollama-url http://localhost:11434] [--memory] [--question "..."]
string[] arguments = args;
bool useOllama = arguments.Contains("--ollama");
string ollamaModel = ValueAfter("--ollama") is { } model && !model.StartsWith("--", StringComparison.Ordinal) ? model : "all-minilm";
var ollamaUrl = new Uri(ValueAfter("--ollama-url") ?? "http://localhost:11434/");
bool inMemory = arguments.Contains("--memory");
string? ownQuestion = ValueAfter("--question");

string manual = ManualCorpus.FindManual(Directory.GetCurrentDirectory())
    ?? ManualCorpus.FindManual(AppContext.BaseDirectory)
    ?? throw new DirectoryNotFoundException("Run the demo from inside the DataVo repository: docs/manual was not found.");

using IEmbeddingGenerator<string, Embedding<float>> generator = useOllama
    ? new OllamaEmbeddingGenerator(ollamaUrl, ollamaModel)
    : new HashingEmbeddingGenerator(dimensions: 384);
EmbeddingGeneratorMetadata? metadata = generator.GetService<EmbeddingGeneratorMetadata>();
Console.WriteLine($"Embedding generator: {metadata?.ProviderName} / {metadata?.DefaultModelId}");

// 1. Load the manual and embed every section in batches (one GenerateAsync call per batch).
List<ManualChunk> chunks = ManualCorpus.Load(manual);
var clock = Stopwatch.StartNew();
const int Batch = 64;
for (int start = 0; start < chunks.Count; start += Batch)
{
    List<ManualChunk> batch = chunks.GetRange(start, Math.Min(Batch, chunks.Count - start));
    GeneratedEmbeddings<Embedding<float>> embeddings = await generator.GenerateAsync(batch.Select(chunk => $"{chunk.Heading}\n{chunk.Text}"));
    for (int i = 0; i < batch.Count; i++)
    {
        batch[i].Embedding = embeddings[i].Vector; // ReadOnlyMemory<float> straight from the generator: no copy
    }
}

int dimensions = chunks[0].Embedding.Length;
Console.WriteLine($"Embedded {chunks.Count} sections of {chunks.Select(chunk => chunk.FilePath).Distinct().Count()} pages ({dimensions} dimensions) in {clock.ElapsedMilliseconds} ms.");

// 2. Store them in a DataVo vector store: LSM on disk (default) or in memory.
string dataDirectory = Path.Combine(Path.GetTempPath(), $"datavo-rag-{Environment.ProcessId}");
var config = inMemory
    ? new DataVoConfig { StorageMode = StorageMode.InMemory }
    : new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = dataDirectory };
try
{
    using (var store = new DataVoVectorStore(config, new DataVoVectorStoreOptions { DatabaseName = "Rag" }))
    {
        var collection = new DataVoVectorStoreCollection<string, ManualChunk>(store, "manual_chunks", ManualChunk.Definition(dimensions));
        await collection.EnsureCollectionDeletedAsync();
        await collection.EnsureCollectionExistsAsync();
        clock.Restart();
        await collection.UpsertAsync(chunks);
        Console.WriteLine($"Upserted into DataVo ({(inMemory ? "InMemory" : "Lsm at " + dataDirectory)}) in {clock.ElapsedMilliseconds} ms.");
        Console.WriteLine();

        // 3. Semantic search, then hybrid search: the same query with metadata filters translated to SQL WHERE.
        string[] questions = ownQuestion is not null
            ? [ownQuestion]
            : ["How do I make writes durable with fsync on disk?", "HNSW index options ef_search and m", "snapshot isolation and write skew in transactions"];
        DateTime since = chunks.Max(chunk => chunk.UpdatedAt).AddYears(-1);
        foreach (string question in questions)
        {
            ReadOnlyMemory<float> query = (await generator.GenerateAsync([question]))[0].Vector;
            Console.WriteLine($"Q: {question}");
            await Print("semantic", collection.SearchAsync(query, top: 3));
            await Print(
                "hybrid (Category = fundamentals, UpdatedAt >= " + since.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + ")",
                collection.SearchAsync(query, top: 3, new VectorSearchOptions<ManualChunk>
                {
                    Filter = chunk => chunk.Category == "fundamentals" && chunk.UpdatedAt >= since,
                }));
            Console.WriteLine();
        }

        // 4. The collection is an ordinary DataVo table: EXPLAIN the hybrid query the store ran.
        ReadOnlyMemory<float> explained = (await generator.GenerateAsync([questions[0]]))[0].Vector;
        string vector = "[" + string.Join(",", explained.ToArray().Select(value => value.ToString("R", CultureInfo.InvariantCulture))) + "]";
        QueryResult plan = store.Context.Execute(
            $"USE Rag; EXPLAIN SELECT Id, Embedding <=> '{vector}' AS distance FROM manual_chunks " +
            "WHERE Category = 'fundamentals' ORDER BY distance ASC LIMIT 3")[^1];
        Console.WriteLine("EXPLAIN of the hybrid query:");
        if (plan.IsError)
        {
            Console.WriteLine("  " + string.Join(" ", plan.Messages));
        }
        else
        {
            foreach (Dictionary<string, object?> row in plan.Data)
            {
                foreach ((string column, object? value) in row)
                {
                    if (Convert.ToString(value, CultureInfo.InvariantCulture) is { Length: > 0 } text)
                    {
                        Console.WriteLine($"  {column,-18} {text}");
                    }
                }
            }
        }
    }

    // 5. LSM persistence: reopen the directory and search again without re-embedding the manual.
    if (!inMemory)
    {
        using var reopened = new DataVoVectorStore(config, new DataVoVectorStoreOptions { DatabaseName = "Rag" });
        var collection = new DataVoVectorStoreCollection<string, ManualChunk>(reopened, "manual_chunks", ManualChunk.Definition(dimensions));
        ReadOnlyMemory<float> query = (await generator.GenerateAsync(["LSM durability"]))[0].Vector;
        VectorSearchResult<ManualChunk> best = await collection.SearchAsync(query, top: 1).SingleAsync();
        Console.WriteLine();
        Console.WriteLine($"After reopen: best match for 'LSM durability' is {best.Record.FilePath} ({best.Record.Heading}).");
    }
}
finally
{
    if (Directory.Exists(dataDirectory))
    {
        Directory.Delete(dataDirectory, recursive: true);
    }
}

Console.WriteLine("RAG demo finished.");
return 0;

static async Task Print(string label, IAsyncEnumerable<VectorSearchResult<ManualChunk>> hits)
{
    Console.WriteLine($"  {label}:");
    int count = 0;
    await foreach (VectorSearchResult<ManualChunk> hit in hits)
    {
        count++;
        string excerpt = hit.Record.Text.Length > 90 ? hit.Record.Text[..90] + "..." : hit.Record.Text;
        Console.WriteLine($"    {hit.Score,6:F3}  {hit.Record.FilePath} > {hit.Record.Heading}");
        Console.WriteLine($"            {excerpt}");
    }

    if (count == 0)
    {
        Console.WriteLine("    (no matches)");
    }
}

string? ValueAfter(string flag)
{
    int index = Array.IndexOf(arguments, flag);
    return index >= 0 && index + 1 < arguments.Length ? arguments[index + 1] : null;
}
