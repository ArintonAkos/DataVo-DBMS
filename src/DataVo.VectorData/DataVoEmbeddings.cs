using Microsoft.Extensions.AI;

namespace DataVo.VectorData;

/// <summary>
/// Runs the embedding generator of a <see cref="DataVoVectorKind.Generated"/> vector property: inputs of the
/// property's type (usually <see cref="string"/>) become <see cref="Embedding{T}"/> of <see cref="float"/>.
/// </summary>
internal static class DataVoEmbeddings
{
    /// <summary>Embeds <paramref name="inputs"/> (NULL inputs stay NULL) with the property's generator.</summary>
    public static async Task<IList<Embedding<float>>> GenerateAsync(DataVoPropertyModel property, IReadOnlyList<object?> inputs, CancellationToken cancellationToken)
    {
        IEmbeddingGenerator generator = property.EmbeddingGenerator
            ?? throw new InvalidOperationException($"Vector property '{property.Name}' has no embedding generator.");
        var results = new Embedding<float>[inputs.Count];
        var present = new List<int>(inputs.Count);
        for (int i = 0; i < inputs.Count; i++)
        {
            if (inputs[i] is not null)
            {
                present.Add(i);
            }
        }

        if (present.Count == 0)
        {
            return results;
        }

        GeneratedEmbeddings<Embedding<float>> generated = generator switch
        {
            IEmbeddingGenerator<string, Embedding<float>> text =>
                await text.GenerateAsync(present.Select(i => Text(property, inputs[i])), options: null, cancellationToken).ConfigureAwait(false),
            _ => throw new NotSupportedException(
                $"Vector property '{property.Name}': the embedding generator must be an IEmbeddingGenerator<string, Embedding<float>>."),
        };

        if (generated.Count != present.Count)
        {
            throw new InvalidOperationException($"The embedding generator returned {generated.Count} embeddings for {present.Count} inputs.");
        }

        for (int i = 0; i < present.Count; i++)
        {
            Embedding<float> embedding = generated[i];
            if (embedding.Vector.Length != property.Dimensions)
            {
                throw new InvalidOperationException(
                    $"The embedding generator returned {embedding.Vector.Length} dimensions; '{property.Name}' declares {property.Dimensions}.");
            }

            results[present[i]] = embedding;
        }

        return results;
    }

    private static string Text(DataVoPropertyModel property, object? input) => input as string
        ?? throw new NotSupportedException(
            $"Vector property '{property.Name}' is a {property.Type.Name}; only string inputs can be embedded by an IEmbeddingGenerator<string, Embedding<float>>.");
}
