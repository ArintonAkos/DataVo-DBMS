using System.Text;
using System.Text.Json;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Options;
using Microsoft.EntityFrameworkCore;
using SemanticInventory.Backend.Data;

namespace SemanticInventory.Backend.Services;

public interface ITextEmbeddingService
{
    Task<Embedding<float>> EmbedAsync(string text, CancellationToken cancellationToken = default);
}

public sealed class EmbeddingOptions
{
    public bool UseOllama { get; set; } = true;
    public string OllamaBaseUrl { get; set; } = "http://127.0.0.1:11434";
    public string OllamaModel { get; set; } = "nomic-embed-text";
    public int OllamaTimeoutSeconds { get; set; } = 15;
}

public sealed class OllamaOrDeterministicEmbeddingService(
    IHttpClientFactory httpClientFactory,
    IOptions<EmbeddingOptions> options) : ITextEmbeddingService
{
    private readonly EmbeddingOptions _options = options.Value;

    public async Task<Embedding<float>> EmbedAsync(string text, CancellationToken cancellationToken = default)
    {
        if (_options.UseOllama)
        {
            float[]? ollamaVector = await TryEmbedWithOllamaAsync(text, cancellationToken);
            if (ollamaVector is not null)
            {
                return new Embedding<float>(ollamaVector);
            }
        }

        return new Embedding<float>(CreateDeterministicVector(text));
    }

    private async Task<float[]?> TryEmbedWithOllamaAsync(string text, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(_options.OllamaBaseUrl) || string.IsNullOrWhiteSpace(_options.OllamaModel))
        {
            return null;
        }

        var client = httpClientFactory.CreateClient("ollama-embeddings");
        if (client.Timeout == Timeout.InfiniteTimeSpan)
        {
            client.Timeout = TimeSpan.FromSeconds(Math.Clamp(_options.OllamaTimeoutSeconds, 3, 60));
        }

        string endpoint = _options.OllamaBaseUrl.TrimEnd('/') + "/api/embeddings";
        var body = JsonSerializer.Serialize(new
        {
            model = _options.OllamaModel,
            prompt = text
        });

        using var request = new HttpRequestMessage(HttpMethod.Post, endpoint)
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json")
        };

        try
        {
            using var response = await client.SendAsync(request, cancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                return null;
            }

            using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
            using JsonDocument doc = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
            if (!TryReadEmbeddingPayload(doc.RootElement, out float[] raw))
            {
                return null;
            }

            return raw;
        }
        catch
        {
            return null;
        }
    }

    private static bool TryReadEmbeddingPayload(JsonElement root, out float[] values)
    {
        values = [];

        if (root.TryGetProperty("embedding", out JsonElement single) && single.ValueKind == JsonValueKind.Array)
        {
            values = ReadFloatArray(single);
            return values.Length > 0;
        }

        if (root.TryGetProperty("embeddings", out JsonElement many)
            && many.ValueKind == JsonValueKind.Array
            && many.GetArrayLength() > 0)
        {
            JsonElement first = many[0];
            if (first.ValueKind == JsonValueKind.Array)
            {
                values = ReadFloatArray(first);
                return values.Length > 0;
            }
        }

        return false;
    }

    private static float[] ReadFloatArray(JsonElement array)
    {
        var result = new List<float>(Math.Max(array.GetArrayLength(), 3));
        foreach (JsonElement value in array.EnumerateArray())
        {
            if (value.ValueKind == JsonValueKind.Number && value.TryGetSingle(out float parsed))
            {
                result.Add(parsed);
            }
        }

        return result.ToArray();
    }

    private static float[] CreateDeterministicVector(string text)
    {
        string candidate = text.Trim().ToLowerInvariant();

        // Keep this for offline fallback only when Ollama is unavailable.
        return
        [
            NormalizedBucket(candidate, 0),
            NormalizedBucket(candidate, 1),
            NormalizedBucket(candidate, 2)
        ];
    }

    private static float NormalizedBucket(string text, int salt)
    {
        unchecked
        {
            int hash = 17 + salt;
            foreach (char ch in text)
            {
                hash = (hash * 31) + ch;
            }

            int positive = Math.Abs(hash % 1000);
            return positive / 1000f;
        }
    }
}

public sealed class EmbeddingPipelineService(
    IDbContextFactory<SemanticInventoryContext> contextFactory,
    ITextEmbeddingService embeddingService)
{
    public async Task<(bool Success, string Message)> EmbedItemAsync(int itemId, string sourceText, CancellationToken cancellationToken = default)
    {
        if (itemId <= 0 || string.IsNullOrWhiteSpace(sourceText))
        {
            return (false, "Item Id and source text are required.");
        }

        Embedding<float> embedding = await embeddingService.EmbedAsync(sourceText, cancellationToken);
        ReadOnlyMemory<float> values = embedding.Vector;

        if (values.Length == 0)
        {
            return (false, "Embedding vector is empty.");
        }

        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);
        var item = await context.Items.SingleOrDefaultAsync(i => i.Id == itemId, cancellationToken);
        if (item == null)
        {
            return (false, $"Item {itemId} not found.");
        }

        var existing = await context.ItemEmbeddings.SingleOrDefaultAsync(e => e.ItemId == itemId, cancellationToken);
        if (existing == null)
        {
            context.ItemEmbeddings.Add(new ItemEmbedding
            {
                ItemId = itemId,
                VectorJson = SerializeVector(values.Span),
                Dimensions = values.Length,
                E0 = values.Span[0],
                E1 = values.Length > 1 ? values.Span[1] : 0f,
                E2 = values.Length > 2 ? values.Span[2] : 0f
            });
        }
        else
        {
            existing.VectorJson = SerializeVector(values.Span);
            existing.Dimensions = values.Length;
            existing.E0 = values.Span[0];
            existing.E1 = values.Length > 1 ? values.Span[1] : 0f;
            existing.E2 = values.Length > 2 ? values.Span[2] : 0f;
        }

        await context.SaveChangesAsync(cancellationToken);
        return (true, $"Embedded item {itemId} using Microsoft.Extensions.AI Embedding<float>.");
    }

    public async Task<int> EmbedAllItemsAsync(bool overwriteExisting = false, CancellationToken cancellationToken = default)
    {
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);

        var items = await context.Items
            .AsNoTracking()
            .OrderBy(i => i.Id)
            .ToListAsync(cancellationToken);

        if (items.Count == 0)
        {
            return 0;
        }

        var existingByItemId = await context.ItemEmbeddings
            .ToDictionaryAsync(e => e.ItemId, cancellationToken);

        int updated = 0;
        foreach (var item in items)
        {
            bool hasExisting = existingByItemId.ContainsKey(item.Id);
            if (hasExisting && !overwriteExisting)
            {
                continue;
            }

            string sourceText = $"{item.Name}. Category: {item.Category}. Description: {item.Description}";
            Embedding<float> embedding = await embeddingService.EmbedAsync(sourceText, cancellationToken);
            ReadOnlyMemory<float> values = embedding.Vector;
            if (values.Length == 0)
            {
                continue;
            }

            if (hasExisting)
            {
                var row = existingByItemId[item.Id];
                row.VectorJson = SerializeVector(values.Span);
                row.Dimensions = values.Length;
                row.E0 = values.Span[0];
                row.E1 = values.Length > 1 ? values.Span[1] : 0f;
                row.E2 = values.Length > 2 ? values.Span[2] : 0f;
            }
            else
            {
                var row = new ItemEmbedding
                {
                    ItemId = item.Id,
                    VectorJson = SerializeVector(values.Span),
                    Dimensions = values.Length,
                    E0 = values.Span[0],
                    E1 = values.Length > 1 ? values.Span[1] : 0f,
                    E2 = values.Length > 2 ? values.Span[2] : 0f
                };

                context.ItemEmbeddings.Add(row);
                existingByItemId[item.Id] = row;
            }

            updated++;
        }

        if (updated > 0)
        {
            await context.SaveChangesAsync(cancellationToken);
        }

        return updated;
    }

    private static string SerializeVector(ReadOnlySpan<float> values)
    {
        float[] copy = values.ToArray();
        return JsonSerializer.Serialize(copy);
    }
}
