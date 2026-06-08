using System.Diagnostics;

namespace DataVo.Benchmarks.Common;

public sealed class LatencyRecorder
{
    private readonly List<double> _samplesMs = [];

    public int Count => _samplesMs.Count;

    public double RateOver(double thresholdMs)
    {
        if (_samplesMs.Count == 0)
        {
            return 0;
        }

        return _samplesMs.Count(sample => sample > thresholdMs) / (double)_samplesMs.Count;
    }

    public void AddElapsed(long startTimestamp)
    {
        long elapsed = Stopwatch.GetTimestamp() - startTimestamp;
        _samplesMs.Add(elapsed * 1000d / Stopwatch.Frequency);
    }

    public long Start() => Stopwatch.GetTimestamp();

    public LatencySummary Snapshot()
    {
        double[] sorted = [.. _samplesMs.OrderBy(x => x)];
        return new LatencySummary(
            Count: sorted.Length,
            P50Ms: Percentile.FromSorted(sorted, 50),
            P90Ms: Percentile.FromSorted(sorted, 90),
            P95Ms: Percentile.FromSorted(sorted, 95),
            P99Ms: Percentile.FromSorted(sorted, 99),
            P999Ms: Percentile.FromSorted(sorted, 99.9),
            MaxMs: sorted.Length == 0 ? 0 : sorted[^1],
            MeanMs: sorted.Length == 0 ? 0 : sorted.Average());
    }
}
