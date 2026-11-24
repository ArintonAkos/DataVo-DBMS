namespace DataVo.Tests.Performance;

/// <summary>
/// Measures the bytes a loop allocates once the code has reached its steady state. Code first runs unoptimized
/// and is recompiled in the background (tiered JIT); on a loaded CI machine that can land inside a measured
/// window and cost a one-off allocation that has nothing to do with the loop. Rounds therefore repeat until one
/// reaches the target or a 5-second budget runs out, and the lowest round is returned. A real per-iteration
/// allocation shows up in every round, so it still fails the assertion.
/// </summary>
/// <example>
/// <code>
/// long bytes = SteadyStateAllocation.LowestRound(() => builder.Build(), iterations: 5_000, target: 0);
/// Assert.Equal(0L, bytes);
/// </code>
/// </example>
internal static class SteadyStateAllocation
{
    /// <summary>Runs <paramref name="iteration"/> in measured rounds and returns the fewest bytes one round allocated.</summary>
    /// <param name="iteration">One iteration of the loop under test.</param>
    /// <param name="iterations">Iterations per measured round (after the same number of warm-up iterations).</param>
    /// <param name="target">The byte count that ends the search early once a round reaches it.</param>
    /// <returns>The lowest bytes allocated by a whole round on the calling thread.</returns>
    public static long LowestRound(Action iteration, int iterations, long target)
    {
        long best = long.MaxValue;
        var budget = System.Diagnostics.Stopwatch.StartNew();
        while (true)
        {
            best = Math.Min(best, MeasureRound(iteration, iterations));
            if (best <= target || budget.Elapsed >= TimeSpan.FromSeconds(5))
            {
                return best;
            }

            Thread.Sleep(50); // give background tier-up a chance between rounds
        }
    }

    private static long MeasureRound(Action iteration, int iterations)
    {
        for (int i = 0; i < iterations; i++)
        {
            iteration();
        }

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iterations; i++)
        {
            iteration();
        }

        return GC.GetAllocatedBytesForCurrentThread() - before;
    }
}
