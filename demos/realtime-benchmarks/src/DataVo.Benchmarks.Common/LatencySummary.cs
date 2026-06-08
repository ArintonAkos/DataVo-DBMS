namespace DataVo.Benchmarks.Common;

public sealed record LatencySummary(
    int Count,
    double P50Ms,
    double P90Ms,
    double P95Ms,
    double P99Ms,
    double P999Ms,
    double MaxMs,
    double MeanMs);
