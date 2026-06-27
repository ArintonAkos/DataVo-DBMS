using System.Diagnostics;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.ConcurrentOps;

internal sealed record ConcurrentWorkerResult(long Operations, List<double> LatenciesMs);

internal static class ConcurrentOpsMetrics
{
    public static ConcurrentOpsResult Build(
        IReadOnlyList<ConcurrentWorkerResult> readResults,
        IReadOnlyList<ConcurrentWorkerResult> writeResults,
        TimeSpan elapsed)
    {
        long readOperations = readResults.Sum(static result => result.Operations);
        long writeOperations = writeResults.Sum(static result => result.Operations);
        double elapsedSeconds = Math.Max(elapsed.TotalSeconds, 0.001d);
        return new ConcurrentOpsResult(
            readOperations,
            writeOperations,
            (readOperations + writeOperations) / elapsedSeconds,
            P99(readResults),
            P99(writeResults),
            elapsed.TotalMilliseconds);
    }

    public static double ElapsedMs(long startTimestamp) =>
        (Stopwatch.GetTimestamp() - startTimestamp) * 1000d / Stopwatch.Frequency;

    private static double P99(IReadOnlyList<ConcurrentWorkerResult> results)
    {
        int count = results.Sum(static result => result.LatenciesMs.Count);
        if (count == 0)
        {
            return 0d;
        }

        var values = new double[count];
        int offset = 0;
        foreach (ConcurrentWorkerResult result in results)
        {
            result.LatenciesMs.CopyTo(values, offset);
            offset += result.LatenciesMs.Count;
        }

        Array.Sort(values);
        int rank = (int)Math.Ceiling(values.Length * 0.99d);
        return values[Math.Clamp(rank - 1, 0, values.Length - 1)];
    }
}
