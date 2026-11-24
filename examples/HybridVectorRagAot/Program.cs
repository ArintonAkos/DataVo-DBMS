using System.Diagnostics;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;
using HybridVectorRagAot;

// Local hybrid retrieval: chunks with metadata (Category, CreatedAt DATETIME) and VECTOR(384) embeddings, an HNSW
// index, and SQL that filters on metadata while ranking by cosine distance. Runs in-memory by default;
// `--data <dir>` keeps the database on disk (LSM engine) between runs.
string? dataDirectory = ArgValue(args, "--data");
string? question = args.FirstOrDefault(a => !a.StartsWith("--", StringComparison.Ordinal) && a != dataDirectory);

var embedder = new HashingEmbedder(dimensions: 384);
var config = dataDirectory is null
    ? new DataVoConfig { StorageMode = StorageMode.InMemory }
    : new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = dataDirectory, LsmStrictFsync = false };

using var db = new DataVoContext(config);
Run(db, "CREATE DATABASE IF NOT EXISTS Rag");
Run(db, "USE Rag");

if (!TableHasRows(db))
{
    var load = Stopwatch.StartNew();
    Run(db, """
        CREATE TABLE chunks (
            Id INT PRIMARY KEY,
            DocumentId INT,
            Category VARCHAR(32),
            CreatedAt DATETIME,
            Text VARCHAR(400),
            Embedding VECTOR(384))
        """);

    DataVoPreparedStatement insert = db.Prepare(
        "INSERT INTO chunks (Id, DocumentId, Category, CreatedAt, Text, Embedding) VALUES (@id, @doc, @category, @created, @text, @embedding)");
    Run(db, "BEGIN");
    foreach (Chunk chunk in Corpus.Chunks())
    {
        Check(insert.Execute(
            ("id", chunk.Id),
            ("doc", chunk.DocumentId),
            ("category", chunk.Category),
            ("created", chunk.CreatedAt),
            ("text", chunk.Text),
            ("embedding", embedder.Embed(chunk.Text))));
    }

    Run(db, "COMMIT");
    Run(db, "CREATE INDEX idx_chunks_category ON chunks (Category)");
    Run(db, "CREATE INDEX idx_chunks_embedding ON chunks (Embedding) USING HNSW WITH (metric = 'cosine', m = 16, ef_construction = 64)");
    Console.WriteLine($"Indexed {Corpus.Chunks().Count} chunks (384-dim embeddings, HNSW) in {load.Elapsed.TotalMilliseconds:F1} ms.");
}

// 1. Pure semantic search: the nearest chunks to the question, whatever their metadata.
string asked = question ?? "how often should we rotate encryption keys";
float[] q = embedder.Embed(asked);
Section($"Semantic top-5 for \"{asked}\"");
Print(Query(db, """
    SELECT Id, Category, CreatedAt, Text, Embedding <=> @q AS Distance
    FROM chunks
    ORDER BY Distance ASC
    LIMIT 5
    """, ("q", q)));

// 2. Hybrid search: the same ranking, restricted by SQL metadata filters and a distance threshold. The stale 2025
//    runbook that wins the pure semantic search is filtered out by date; the current 2026 policy is returned.
var since = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
const string Hybrid = """
    SELECT Id, Category, CreatedAt, Text, Embedding <=> @q AS Distance
    FROM chunks
    WHERE Category = @category
      AND CreatedAt >= @since
      AND Embedding <=> @q < 0.95
    ORDER BY Distance ASC
    LIMIT 5
    """;
Section($"Hybrid: Category = 'security' AND CreatedAt >= {since:yyyy-MM-dd} AND distance < 0.95");
Print(Query(db, Hybrid, ("q", q), ("category", "security"), ("since", since)));

// 3. How the hybrid query executes, straight from EXPLAIN.
Section("EXPLAIN (hybrid query)");
QueryResult plan = db.Prepare("EXPLAIN " + Hybrid).Execute(("q", q), ("category", "security"), ("since", since));
Check(plan);
foreach (string column in new[] { "Operator", "FastPath", "FallbackReason", "DeclinedPaths", "EstimatedRows", "RowsReturned", "IndexesUsed", "Details" })
{
    Console.WriteLine($"  {column,-15} {plan.Data[0][column] ?? "-"}");
}

// 4. Latency of the hybrid query, warm.
Section("Hybrid query latency (warm, 1,000 runs)");
DataVoPreparedStatement hybrid = db.Prepare(Hybrid);
for (int i = 0; i < 100; i++)
{
    hybrid.Execute(("q", q), ("category", "security"), ("since", since));
}

var timer = Stopwatch.StartNew();
for (int i = 0; i < 1_000; i++)
{
    hybrid.Execute(("q", q), ("category", "security"), ("since", since));
}

Console.WriteLine($"  {timer.Elapsed.TotalMicroseconds / 1_000:F1} µs per query on {(System.Runtime.JitInfo.GetCompiledMethodCount() > 0 ? "the JIT" : "Native AOT")}.");
return 0;

static List<Dictionary<string, object?>> Query(DataVoContext db, string sql, params DataVoQueryParameter[] parameters)
{
    QueryResult result = db.Prepare(sql).Execute(parameters);
    Check(result);
    return result.Data;
}

static void Print(List<Dictionary<string, object?>> rows)
{
    if (rows.Count == 0)
    {
        Console.WriteLine("  (no rows)");
    }

    foreach (Dictionary<string, object?> row in rows)
    {
        string text = (string)row["Text"]!;
        Console.WriteLine($"  #{row["Id"],-3} {Convert.ToDouble(row["Distance"]):F3}  {row["Category"],-10} {(DateTime)row["CreatedAt"]!:yyyy-MM-dd}  {(text.Length > 88 ? text[..85] + "..." : text)}");
    }
}

static void Section(string title) => Console.WriteLine($"{Environment.NewLine}== {title}");

static bool TableHasRows(DataVoContext db)
{
    QueryResult result = db.Execute("SELECT COUNT(*) AS N FROM chunks").Last();
    return !result.IsError && Convert.ToInt64(result.Data[0]["N"]) > 0;
}

static void Run(DataVoContext db, string sql) => Check(db.Execute(sql).Last());

static void Check(QueryResult result)
{
    if (result.IsError)
    {
        throw new InvalidOperationException(string.Join(" | ", result.Messages));
    }
}

static string? ArgValue(string[] args, string name)
{
    int at = Array.IndexOf(args, name);
    return at >= 0 && at + 1 < args.Length ? args[at + 1] : null;
}
