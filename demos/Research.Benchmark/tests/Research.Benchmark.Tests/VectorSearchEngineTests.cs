using Research.Benchmark.Abstractions;
using Research.Benchmark.Runners.VectorSearch;

namespace Research.Benchmark.Tests;

/// <summary>
/// Correctness contract for the Scenario C (Vector Search) engines: querying with an exact copy of an
/// inserted vector must return that vector's id among the Top-K nearest. SQLite is skipped when the native
/// sqlite-vec extension is not available in the environment.
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

    [Fact]
    public void SqliteVec_ExactMatchQuery_ReturnsPlantedVector_WhenExtensionAvailable()
    {
        using var engine = new SqliteVectorSearchEngine();
        try
        {
            engine.Initialize(Dimensions);
        }
        catch (InvalidOperationException)
        {
            return; // sqlite-vec extension not available in this environment — skip.
        }

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
