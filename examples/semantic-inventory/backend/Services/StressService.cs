using System.Diagnostics;
using SemanticInventory.Backend.Data;
using Microsoft.EntityFrameworkCore;

namespace SemanticInventory.Backend.Services;

public sealed class StressService(
    IDbContextFactory<SemanticInventoryContext> contextFactory,
    InventoryService inventoryService)
{
    private sealed record EmbeddingPoint(int ItemId, float E0, float E1, float E2);

    public sealed record StressResult(int Iterations, int Successes, int Failures, double AvgMs, double P95Ms, int OrdersPlaced);

    public async Task<StressResult> RunAsync(int iterations, CancellationToken cancellationToken = default)
    {
        int safeIterations = Math.Clamp(iterations, 1, 2000);
        var random = new Random(42);

        await using var preloadContext = await contextFactory.CreateDbContextAsync(cancellationToken);
        var points = await preloadContext.ItemEmbeddings
            .AsNoTracking()
            .Select(e => new EmbeddingPoint(e.ItemId, e.E0, e.E1, e.E2))
            .ToListAsync(cancellationToken);

        if (points.Count == 0)
        {
            return new StressResult(0, 0, 0, 0d, 0d, 0);
        }

        var durations = new List<double>(safeIterations);
        int successes = 0;
        int failures = 0;
        int ordersPlaced = 0;

        for (int i = 0; i < safeIterations; i++)
        {
            var sw = Stopwatch.StartNew();
            try
            {
                float q0 = (float)random.NextDouble();
                float q1 = (float)random.NextDouble();
                float q2 = (float)random.NextDouble();

                var nearest = points
                    .Select(point => new
                    {
                        point.ItemId,
                        Score = CosineSimilarity3(q0, q1, q2, point.E0, point.E1, point.E2)
                    })
                    .OrderByDescending(row => row.Score)
                    .Take(3)
                    .ToList();

                if (nearest.Count > 0 && i % 5 == 0)
                {
                    int itemId = nearest[0].ItemId;
                    var orderResult = await inventoryService.PlaceOrderAsync(itemId, 1, cancellationToken);
                    if (orderResult.Success)
                    {
                        ordersPlaced++;
                    }
                }

                successes++;
            }
            catch
            {
                failures++;
            }
            finally
            {
                sw.Stop();
                durations.Add(sw.Elapsed.TotalMilliseconds);
            }
        }

        durations.Sort();
        double avg = durations.Count == 0 ? 0d : durations.Average();
        int p95Index = durations.Count == 0 ? 0 : (int)Math.Ceiling(durations.Count * 0.95) - 1;
        p95Index = Math.Clamp(p95Index, 0, Math.Max(0, durations.Count - 1));
        double p95 = durations.Count == 0 ? 0d : durations[p95Index];

        // Optional integrity check read to ensure EF query path stayed healthy post-stress.
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);
        _ = await context.Items.AsNoTracking().CountAsync(cancellationToken);

        return new StressResult(safeIterations, successes, failures, avg, p95, ordersPlaced);
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
