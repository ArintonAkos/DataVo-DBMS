using Microsoft.Extensions.AI;
using Microsoft.Extensions.VectorData;

namespace DataVo.VectorData.Tests;

/// <summary>A document with an HNSW cosine-similarity vector; the property named <c>Vector</c> is a reserved SQL word.</summary>
public sealed class Doc
{
    [VectorStoreKey]
    public int Id { get; set; }

    [VectorStoreData(IsIndexed = true)]
    public string Category { get; set; } = "";

    [VectorStoreData]
    public string FilePath { get; set; } = "";

    [VectorStoreData(IsIndexed = true)]
    public DateTime UpdatedAt { get; set; }

    [VectorStoreData]
    public int Rank { get; set; }

    [VectorStoreData]
    public bool Published { get; set; }

    [VectorStoreData]
    public string? Note { get; set; }

    [VectorStoreData]
    public string[] Tags { get; set; } = [];

    [VectorStoreVector(4, DistanceFunction = DistanceFunction.CosineSimilarity, IndexKind = IndexKind.Hnsw)]
    public ReadOnlyMemory<float> Vector { get; set; }
}

/// <summary>A FLAT Euclidean index over a <c>float[]</c> vector, keyed by <c>long</c>.</summary>
public sealed class FlatDoc
{
    [VectorStoreKey]
    public long Id { get; set; }

    [VectorStoreData]
    public string Name { get; set; } = "";

    [VectorStoreVector(3, DistanceFunction = DistanceFunction.EuclideanDistance, IndexKind = IndexKind.Flat)]
    public float[]? Embedding { get; set; }
}

/// <summary>An <see cref="Embedding{T}"/> vector with cosine distance, keyed by an auto-generated <see cref="Guid"/>.</summary>
public sealed class GuidDoc
{
    [VectorStoreKey]
    public Guid Id { get; set; }

    [VectorStoreData]
    public string Title { get; set; } = "";

    [VectorStoreVector(3, DistanceFunction = DistanceFunction.CosineDistance)]
    public Embedding<float>? Embedding { get; set; }
}

/// <summary>A string key and a nullable <see cref="ReadOnlyMemory{T}"/> vector with squared Euclidean distance.</summary>
public sealed class StringKeyDoc
{
    [VectorStoreKey]
    public string Key { get; set; } = "";

    [VectorStoreData]
    public double Score { get; set; }

    [VectorStoreVector(2, DistanceFunction = DistanceFunction.EuclideanSquaredDistance, IndexKind = IndexKind.Flat)]
    public ReadOnlyMemory<float>? Point { get; set; }
}

/// <summary>A document whose vector is generated from its text by the store's embedding generator.</summary>
public sealed class TextDoc
{
    [VectorStoreKey]
    public int Id { get; set; }

    [VectorStoreData]
    public string Body { get; set; } = "";

    [VectorStoreVector(TestEmbeddingGenerator.Dimensions)]
    public string Text { get; set; } = "";
}

/// <summary>
/// A deterministic <see cref="IEmbeddingGenerator{TInput, TEmbedding}"/>: each vector counts the letters a-h of the
/// text, so texts sharing letters are close.
/// </summary>
public sealed class TestEmbeddingGenerator : IEmbeddingGenerator<string, Embedding<float>>
{
    public const int Dimensions = 8;

    public int Calls { get; private set; }

    public Task<GeneratedEmbeddings<Embedding<float>>> GenerateAsync(
        IEnumerable<string> values,
        EmbeddingGenerationOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        Calls++;
        var result = new GeneratedEmbeddings<Embedding<float>>();
        foreach (string value in values)
        {
            var vector = new float[Dimensions];
            foreach (char c in value.ToLowerInvariant())
            {
                if (c is >= 'a' and < (char)('a' + Dimensions))
                {
                    vector[c - 'a'] += 1;
                }
            }

            vector[^1] += 0.01f; // never all zeros
            result.Add(new Embedding<float>(vector));
        }

        return Task.FromResult(result);
    }

    public object? GetService(Type serviceType, object? serviceKey = null) => null;

    public void Dispose()
    {
    }
}
