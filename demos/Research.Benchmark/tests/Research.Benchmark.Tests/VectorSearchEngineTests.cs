using Research.Benchmark.Abstractions;
using Research.Benchmark.Runners.VectorSearch;

namespace Research.Benchmark.Tests;

/// <summary>
/// Correctness contract for the Scenario C (Vector Search) engines: querying with an exact copy of an
/// inserted vector must return that vector's id among the Top-K nearest. The SQLite test is reported as
/// skipped unless <c>SQLITE_VEC_PATH</c> points at the native sqlite-vec extension.
/// </summary>
public sealed class VectorSearchEngineTests
{
    private const int Dimensions = 16;
    private const int Count = 200;

    public static IEnumerable<object[]> AlwaysAvailableEngines()
    {
        yield return [new DataVoVectorSearchEngine()];
        yield return [new DataVoVectorSearchEngine("FLAT", "DataVo-Flat")];
        yield return [new LiteDbVectorSearchEngine()];
    }

    [Fact]
    public void DataVoVectorSearchEngine_FlatVariant_HasDistinctBenchmarkName()
    {
        using var engine = new DataVoVectorSearchEngine("FLAT", "DataVo-Flat");

        Assert.Equal("DataVo-Flat [DataVo.Core net10.0]", engine.Name);
    }

    [Fact]
    public void DataVoVectorSearchEngine_DiversityProfileVariant_HasDistinctBenchmarkNameAndDiagnostics()
    {
        using var engine = new DataVoVectorSearchEngine(
            "HNSW",
            "DataVo-HNSW-Diversity",
            expectedVectors: Count,
            enableDiversityHeuristic: true,
            enableBuildDiagnostics: true);

        Assert.Equal("DataVo-HNSW-Diversity [DataVo.Core net10.0]", engine.Name);

        engine.Initialize(Dimensions);
        float[][] vectors = SeedVectors(engine);

        IReadOnlyList<long> top = engine.Search(vectors[41], k: 5);

        Assert.Contains(42L, top);
        Assert.True(engine.TryFormatBuildDiagnostics(out string diagnostics));
        Assert.Contains("diversityComparisons=", diagnostics);
    }

    [Theory]
    [MemberData(nameof(AlwaysAvailableEngines))]
    public void ExactMatchQueryReturnsPlantedVector(IVectorSearchEngine engine)
    {
        using (engine)
        {
            engine.Initialize(Dimensions);
            float[][] vectors = SeedVectors(engine);

            IReadOnlyList<long> top = engine.Search(vectors[41], k: 5);

            Assert.Contains(42L, top); // vector index 41 was inserted with id 42
        }
    }

    [SqliteVecFact]
    public void SqliteVec_ExactMatchQuery_ReturnsPlantedVector_WhenExtensionAvailable()
    {
        using var engine = new SqliteVectorSearchEngine();
        engine.Initialize(Dimensions);

        float[][] vectors = SeedVectors(engine);
        IReadOnlyList<long> top = engine.Search(vectors[41], k: 5);
        Assert.Contains(42L, top);
    }

    private static float[][] SeedVectors(IVectorSearchEngine engine)
    {
        var rng = new Random(7);
        var vectors = new float[Count][];
        engine.BeginBatch();
        for (int i = 0; i < Count; i++)
        {
            vectors[i] = RandomUnitVector(rng, Dimensions);
            engine.Insert(i + 1, vectors[i]);
        }

        engine.CompleteBatch();
        return vectors;
    }

    private static float[] RandomUnitVector(Random rng, int dimensions)
    {
        var vector = new float[dimensions];
        double sumSquares = 0d;
        for (int i = 0; i < dimensions; i++)
        {
            float value = (float)(rng.NextDouble() * 2d - 1d);
            vector[i] = value;
            sumSquares += value * (double)value;
        }

        float norm = (float)Math.Sqrt(sumSquares);
        for (int i = 0; i < dimensions; i++)
        {
            vector[i] /= norm;
        }

        return vector;
    }
}

/// <summary>
/// A <see cref="FactAttribute"/> that runs only when <c>SQLITE_VEC_PATH</c> names an existing sqlite-vec
/// loadable library; otherwise the runner reports the test as skipped rather than as passed.
/// </summary>
public sealed class SqliteVecFactAttribute : FactAttribute
{
    /// <summary>Initializes the attribute and skips the test when the extension path is missing.</summary>
    public SqliteVecFactAttribute()
    {
        string? path = Environment.GetEnvironmentVariable("SQLITE_VEC_PATH");
        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
        {
            Skip = "sqlite-vec is not available: set SQLITE_VEC_PATH to a vec0 loadable library to run this test.";
        }
    }
}
