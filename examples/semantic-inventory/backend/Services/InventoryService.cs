using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.AI;
using System.Text.Json;
using SemanticInventory.Backend.Data;

namespace SemanticInventory.Backend.Services;

public sealed class InventoryService(
    IDbContextFactory<SemanticInventoryContext> contextFactory,
    ITextEmbeddingService embeddingService)
{
    public sealed record CatalogRow(int Id, string Name, string Category, decimal Price, int Quantity, string Location);
    public sealed record TopSellerRow(int ItemId, string Name, int UnitsSold, decimal Revenue);
    public sealed record SemanticRow(int ItemId, string Name, string Category, decimal Price, int Quantity, double Score);

    public async Task<List<CatalogRow>> GetCatalogAsync(string? categoryFilter = null, CancellationToken cancellationToken = default)
    {
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);

        IQueryable<Item> query = context.Items
            .AsNoTracking()
            .Include(item => item.Inventory);

        if (!string.IsNullOrWhiteSpace(categoryFilter))
        {
            query = query.Where(item => item.Category == categoryFilter);
        }

        return await query
            .OrderBy(item => item.Name)
            .Select(item => new CatalogRow(
                item.Id,
                item.Name,
                item.Category,
                item.Price,
                item.Inventory != null ? item.Inventory.Quantity : 0,
                item.Inventory != null ? item.Inventory.Location : "-"))
            .ToListAsync(cancellationToken);
    }

    public async Task<List<TopSellerRow>> GetTopSellersAsync(int take = 5, CancellationToken cancellationToken = default)
    {
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);

        int safeTake = Math.Clamp(take, 1, 50);

        var rows = await context.Sales
            .AsNoTracking()
            .GroupBy(sale => sale.ItemId)
            .Select(group => new
            {
                ItemId = group.Key,
                UnitsSold = group.Sum(sale => sale.Qty),
                Revenue = group.Sum(sale => sale.Qty * sale.UnitPrice)
            })
            .Join(
                context.Items.AsNoTracking(),
                aggregated => aggregated.ItemId,
                item => item.Id,
                (aggregated, item) => new
                {
                    aggregated.ItemId,
                    item.Name,
                    aggregated.UnitsSold,
                    aggregated.Revenue
                })
            .OrderByDescending(row => row.UnitsSold)
            .ThenBy(row => row.Name)
            .Take(safeTake)
            .ToListAsync(cancellationToken);

        return rows
            .Select(row => new TopSellerRow(row.ItemId, row.Name, row.UnitsSold, row.Revenue))
            .ToList();
    }

    public Task<List<SemanticRow>> SearchByEmbeddingAsync(float q0, float q1, float q2, int topK = 5, CancellationToken cancellationToken = default)
    {
        return SearchByEmbeddingAsync([q0, q1, q2], topK, cancellationToken);
    }

    public async Task<List<SemanticRow>> SearchByEmbeddingAsync(IReadOnlyList<float> queryVector, int topK = 5, CancellationToken cancellationToken = default)
    {
        if (queryVector.Count == 0)
        {
            return [];
        }

        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);

        int safeTopK = Math.Clamp(topK, 1, 50);

        if (queryVector.Count <= 3)
        {
            float q0 = queryVector[0];
            float q1 = queryVector.Count > 1 ? queryVector[1] : 0f;
            float q2 = queryVector.Count > 2 ? queryVector[2] : 0f;

            var joined3 = await context.ItemEmbeddings
                .AsNoTracking()
                .Join(context.Items.AsNoTracking(), embedding => embedding.ItemId, item => item.Id, (embedding, item) => new { embedding, item })
                .Join(context.Inventories.AsNoTracking(), pair => pair.item.Id, inventory => inventory.ItemId, (pair, inventory) => new
                {
                    pair.item.Id,
                    pair.item.Name,
                    pair.item.Category,
                    pair.item.Price,
                    inventory.Quantity,
                    pair.embedding.E0,
                    pair.embedding.E1,
                    pair.embedding.E2
                })
                .ToListAsync(cancellationToken);

            return joined3
                .Select(row => new SemanticRow(
                    row.Id,
                    row.Name,
                    row.Category,
                    row.Price,
                    row.Quantity,
                    CosineSimilarity3(q0, q1, q2, row.E0, row.E1, row.E2)))
                .OrderByDescending(row => row.Score)
                .ThenBy(row => row.Name)
                .Take(safeTopK)
                .ToList();
        }

        var joined = await context.ItemEmbeddings
            .AsNoTracking()
            .Join(context.Items.AsNoTracking(), embedding => embedding.ItemId, item => item.Id, (embedding, item) => new { embedding, item })
            .Join(context.Inventories.AsNoTracking(), pair => pair.item.Id, inventory => inventory.ItemId, (pair, inventory) => new
            {
                pair.item.Id,
                pair.item.Name,
                pair.item.Category,
                pair.item.Price,
                inventory.Quantity,
                pair.embedding.VectorJson,
                pair.embedding.Dimensions,
                pair.embedding.E0,
                pair.embedding.E1,
                pair.embedding.E2
            })
            .ToListAsync(cancellationToken);

        var query = queryVector as float[] ?? queryVector.ToArray();

        return joined
            .Select(row =>
            {
                float[] doc = ParseStoredVector(row.VectorJson, row.Dimensions, row.E0, row.E1, row.E2);
                return new SemanticRow(
                    row.Id,
                    row.Name,
                    row.Category,
                    row.Price,
                    row.Quantity,
                    CosineSimilarity(query, doc));
            })
            .OrderByDescending(row => row.Score)
            .ThenBy(row => row.Name)
            .Take(safeTopK)
            .ToList();
    }

    public async Task<List<SemanticRow>> SearchByTextAsync(string query, int topK = 5, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(query))
        {
            return [];
        }

        Embedding<float> embedding = await embeddingService.EmbedAsync(query, cancellationToken);
        ReadOnlyMemory<float> values = embedding.Vector;
        if (values.Length == 0)
        {
            return [];
        }

        return await SearchByEmbeddingAsync(values.ToArray(), topK, cancellationToken);
    }

    public async Task<(bool Success, string Message)> PlaceOrderAsync(int itemId, int qty, CancellationToken cancellationToken = default)
    {
        if (itemId <= 0 || qty <= 0)
        {
            return (false, "Item and quantity must be positive.");
        }

        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);
        await using var transaction = await context.Database.BeginTransactionAsync(cancellationToken);

        var inventory = await context.Inventories.SingleOrDefaultAsync(inventory => inventory.ItemId == itemId, cancellationToken);
        var item = await context.Items.SingleOrDefaultAsync(i => i.Id == itemId, cancellationToken);

        if (inventory == null || item == null)
        {
            return (false, "Item not found.");
        }

        if (inventory.Quantity < qty)
        {
            return (false, $"Insufficient stock. Available: {inventory.Quantity}");
        }

        inventory.Quantity -= qty;
        context.Sales.Add(new Sale
        {
            Id = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            ItemId = itemId,
            Qty = qty,
            UnitPrice = item.Price,
            OccurredUtc = DateTime.UtcNow
        });

        await context.SaveChangesAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);

        return (true, $"Order placed. Remaining stock: {inventory.Quantity}");
    }

    private static float[] ParseStoredVector(string vectorJson, int dimensions, float e0, float e1, float e2)
    {
        if (!string.IsNullOrWhiteSpace(vectorJson))
        {
            try
            {
                float[]? parsed = JsonSerializer.Deserialize<float[]>(vectorJson);
                if (parsed is { Length: > 0 })
                {
                    return parsed;
                }
            }
            catch
            {
                // Fall through to compatibility path for legacy rows.
            }
        }

        if (dimensions > 0)
        {
            return [e0, e1, e2];
        }

        return [];
    }

    private static double CosineSimilarity(IReadOnlyList<float> query, IReadOnlyList<float> doc)
    {
        int length = Math.Min(query.Count, doc.Count);
        if (length == 0)
        {
            return 0d;
        }

        double dot = 0d;
        double qNormSquared = 0d;
        double dNormSquared = 0d;

        for (int i = 0; i < length; i++)
        {
            float q = query[i];
            float d = doc[i];
            dot += q * d;
            qNormSquared += q * q;
            dNormSquared += d * d;
        }

        double qNorm = Math.Sqrt(qNormSquared);
        double dNorm = Math.Sqrt(dNormSquared);

        if (qNorm <= 0d || dNorm <= 0d)
        {
            return 0d;
        }

        return dot / (qNorm * dNorm);
    }

    private static double CosineSimilarity3(float q0, float q1, float q2, float e0, float e1, float e2)
    {
        double dot = (q0 * e0) + (q1 * e1) + (q2 * e2);
        double qNorm = Math.Sqrt((q0 * q0) + (q1 * q1) + (q2 * q2));
        double eNorm = Math.Sqrt((e0 * e0) + (e1 * e1) + (e2 * e2));

        if (qNorm <= 0d || eNorm <= 0d)
        {
            return 0d;
        }

        return dot / (qNorm * eNorm);
    }
}
