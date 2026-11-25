using System.Net.Http.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.AI;

namespace MicrosoftAiLocalRag;

/// <summary>
/// An <see cref="IEmbeddingGenerator{TInput, TEmbedding}"/> over a local Ollama server's <c>/api/embed</c> endpoint
/// (default <c>http://localhost:11434</c>), with no client library: one POST per batch, JSON through source-generated
/// metadata. Pull a model first, for example <c>ollama pull all-minilm</c>.
/// </summary>
public sealed class OllamaEmbeddingGenerator(Uri endpoint, string model) : IEmbeddingGenerator<string, Embedding<float>>
{
    private readonly HttpClient _http = new() { BaseAddress = endpoint, Timeout = TimeSpan.FromMinutes(5) };
    private readonly EmbeddingGeneratorMetadata _metadata = new("ollama", endpoint, model);

    /// <inheritdoc />
    public async Task<GeneratedEmbeddings<Embedding<float>>> GenerateAsync(
        IEnumerable<string> values,
        EmbeddingGenerationOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var request = new EmbedRequest(options?.ModelId ?? model, [.. values]);
        using HttpResponseMessage response = await _http.PostAsJsonAsync("api/embed", request, OllamaJson.Default.EmbedRequest, cancellationToken).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
        EmbedResponse body = await response.Content.ReadFromJsonAsync(OllamaJson.Default.EmbedResponse, cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException("Ollama returned an empty response.");
        if (body.Embeddings.Length != request.Input.Length)
        {
            throw new InvalidOperationException($"Ollama returned {body.Embeddings.Length} embeddings for {request.Input.Length} inputs.");
        }

        var embeddings = new GeneratedEmbeddings<Embedding<float>>();
        foreach (float[] vector in body.Embeddings)
        {
            embeddings.Add(new Embedding<float>(vector) { ModelId = body.Model ?? request.Model });
        }

        return embeddings;
    }

    /// <inheritdoc />
    public object? GetService(Type serviceType, object? serviceKey = null) =>
        serviceKey is null && serviceType == typeof(EmbeddingGeneratorMetadata) ? _metadata
        : serviceKey is null && serviceType.IsInstanceOfType(this) ? this
        : null;

    /// <inheritdoc />
    public void Dispose() => _http.Dispose();

    internal sealed record EmbedRequest(
        [property: JsonPropertyName("model")] string Model,
        [property: JsonPropertyName("input")] string[] Input);

    internal sealed record EmbedResponse(
        [property: JsonPropertyName("model")] string? Model,
        [property: JsonPropertyName("embeddings")] float[][] Embeddings);
}

[JsonSerializable(typeof(OllamaEmbeddingGenerator.EmbedRequest))]
[JsonSerializable(typeof(OllamaEmbeddingGenerator.EmbedResponse))]
internal sealed partial class OllamaJson : JsonSerializerContext;
