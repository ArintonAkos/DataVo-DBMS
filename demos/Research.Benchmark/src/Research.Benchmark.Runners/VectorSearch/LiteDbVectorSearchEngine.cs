using LiteDB;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.VectorSearch;

/// <summary>
/// LiteDB vector-search engine: LiteDB has no vector index, so search is brute force — read the whole
/// collection and compute cosine similarity to the query in memory, then take the Top-K. In-memory over a
/// <see cref="MemoryStream"/>; inserts in one transaction.
/// </summary>
public sealed class LiteDbVectorSearchEngine : IVectorSearchEngine
{
    private LiteDatabase? _database;
    private ILiteCollection<VectorDocument>? _collection;

    public string Name => "LiteDB";

    public void Initialize(int dimensions)
    {
        _database?.Dispose();
        _database = new LiteDatabase(new MemoryStream());
        _collection = _database.GetCollection<VectorDocument>("vectors");
    }

    public void BeginBatch() => Database().BeginTrans();

    public void CompleteBatch() => Database().Commit();

    public void Insert(long id, float[] vector) =>
        Collection().Insert(new VectorDocument { Id = id, Vector = vector });

    public IReadOnlyList<long> Search(float[] query, int k)
    {
        double queryNorm = Norm(query);

        // Brute force: score every stored vector by cosine similarity, keep the Top-K.
        var scored = new List<(long Id, double Similarity)>();
        foreach (VectorDocument document in Collection().FindAll())
        {
            scored.Add((document.Id, CosineSimilarity(query, queryNorm, document.Vector)));
        }

        return scored
            .OrderByDescending(entry => entry.Similarity)
            .Take(k)
            .Select(entry => entry.Id)
            .ToList();
    }

    public void Dispose()
    {
        _database?.Dispose();
        _database = null;
        _collection = null;
    }

    private static double CosineSimilarity(float[] query, double queryNorm, float[] candidate)
    {
        double dot = 0d;
        double candidateNorm = 0d;
        for (int i = 0; i < candidate.Length; i++)
        {
            dot += query[i] * candidate[i];
            candidateNorm += candidate[i] * (double)candidate[i];
        }

        double denominator = queryNorm * Math.Sqrt(candidateNorm);
        return denominator == 0d ? 0d : dot / denominator;
    }

    private static double Norm(float[] vector)
    {
        double sum = 0d;
        foreach (float value in vector)
        {
            sum += value * (double)value;
        }

        return Math.Sqrt(sum);
    }

    private ILiteCollection<VectorDocument> Collection() =>
        _collection ?? throw new InvalidOperationException("LiteDB vector engine has not been initialized.");

    private LiteDatabase Database() =>
        _database ?? throw new InvalidOperationException("LiteDB vector engine has not been initialized.");

    private sealed class VectorDocument
    {
        public long Id { get; set; }
        public float[] Vector { get; set; } = [];
    }
}
