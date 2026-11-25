using System.Text;
using Microsoft.Extensions.AI;

namespace MicrosoftAiLocalRag;

/// <summary>
/// A deterministic, offline <see cref="IEmbeddingGenerator{TInput, TEmbedding}"/>: the "hashing trick". Each lower-cased
/// word and each pair of adjacent words is hashed (FNV-1a) into one of <see cref="Dimensions"/> buckets with a hashed
/// sign, weighted by <c>1 + log(count)</c>, and the vector is L2-normalized. Texts sharing vocabulary get a high cosine
/// similarity, so the demo runs with no model download, no network and no API key. It is lexical, not semantic: swap in
/// a real model (for example <c>--ollama</c>) for paraphrase-level matching.
/// </summary>
public sealed class HashingEmbeddingGenerator(int dimensions = 384) : IEmbeddingGenerator<string, Embedding<float>>
{
    private static readonly HashSet<string> StopWords = new(StringComparer.Ordinal)
    {
        "a", "an", "and", "are", "as", "at", "be", "by", "can", "for", "from", "how", "i", "in", "is", "it", "its",
        "of", "on", "or", "that", "the", "this", "to", "use", "what", "when", "which", "with", "you", "your",
    };

    private readonly EmbeddingGeneratorMetadata _metadata = new("datavo-hashing", defaultModelId: $"hashing-{dimensions}", defaultModelDimensions: dimensions);

    /// <summary>The number of dimensions of every embedding.</summary>
    public int Dimensions { get; } = dimensions;

    /// <inheritdoc />
    public Task<GeneratedEmbeddings<Embedding<float>>> GenerateAsync(
        IEnumerable<string> values,
        EmbeddingGenerationOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var embeddings = new GeneratedEmbeddings<Embedding<float>>();
        foreach (string value in values)
        {
            cancellationToken.ThrowIfCancellationRequested();
            embeddings.Add(new Embedding<float>(Embed(value)) { ModelId = _metadata.DefaultModelId });
        }

        return Task.FromResult(embeddings);
    }

    /// <inheritdoc />
    public object? GetService(Type serviceType, object? serviceKey = null) =>
        serviceKey is null && serviceType == typeof(EmbeddingGeneratorMetadata) ? _metadata
        : serviceKey is null && serviceType.IsInstanceOfType(this) ? this
        : null;

    /// <inheritdoc />
    public void Dispose()
    {
    }

    private float[] Embed(string text)
    {
        var counts = new Dictionary<string, int>(StringComparer.Ordinal);
        string? previous = null;
        foreach (string word in Words(text))
        {
            Add(counts, word);
            if (previous is not null)
            {
                Add(counts, previous + " " + word);
            }

            previous = word;
        }

        var vector = new float[Dimensions];
        foreach ((string term, int count) in counts)
        {
            uint hash = Fnv1a(term);
            float sign = (hash & 0x8000_0000) == 0 ? 1f : -1f;
            float weight = (1f + MathF.Log(count)) * (term.Contains(' ') ? 0.5f : 1f);
            vector[(int)(hash % (uint)Dimensions)] += sign * weight;
        }

        float norm = MathF.Sqrt(vector.Sum(component => component * component));
        if (norm == 0)
        {
            vector[0] = 1; // an empty text still gets a valid unit vector
            return vector;
        }

        for (int i = 0; i < vector.Length; i++)
        {
            vector[i] /= norm;
        }

        return vector;
    }

    private static IEnumerable<string> Words(string text)
    {
        var word = new StringBuilder();
        foreach (char c in text)
        {
            if (char.IsLetterOrDigit(c))
            {
                word.Append(char.ToLowerInvariant(c));
                continue;
            }

            if (word.Length > 0)
            {
                string token = word.ToString();
                word.Clear();
                if (!StopWords.Contains(token))
                {
                    yield return token;
                }
            }
        }

        if (word.Length > 0 && !StopWords.Contains(word.ToString()))
        {
            yield return word.ToString();
        }
    }

    private static void Add(Dictionary<string, int> counts, string term) =>
        counts[term] = counts.TryGetValue(term, out int count) ? count + 1 : 1;

    private static uint Fnv1a(string term)
    {
        uint hash = 2166136261;
        foreach (char c in term)
        {
            hash = (hash ^ c) * 16777619;
        }

        return hash;
    }
}
