using System.Globalization;
using System.Text;

namespace Research.Benchmark.Host;

public sealed record BenchmarkMetrics(
    string EngineName,
    double TotalExecutionTimeMs,
    double P50LatencyMs,
    double P99LatencyMs,
    double TotalGcAllocatedMb);

public static class BenchmarkMetricsCalculator
{
    public static (double P50, double P99) CalculatePercentiles(IReadOnlyList<double> iterationLatenciesMs)
    {
        if (iterationLatenciesMs.Count == 0)
        {
            return (0d, 0d);
        }

        double[] sorted = iterationLatenciesMs.ToArray();
        Array.Sort(sorted);
        return (NearestRank(sorted, 0.50d), NearestRank(sorted, 0.99d));
    }

    private static double NearestRank(double[] sorted, double percentile)
    {
        int rank = (int)Math.Ceiling(percentile * sorted.Length);
        int index = Math.Clamp(rank - 1, 0, sorted.Length - 1);
        return sorted[index];
    }
}

public static class BenchmarkReportFormatter
{
    public static string ToMarkdown(IEnumerable<BenchmarkMetrics> rows)
    {
        var builder = new StringBuilder();
        builder.AppendLine("| Engine Name | Total Execution Time (ms) | P50 Latency (ms) | P99 Latency (ms) | Total GC Allocated (MB) |");
        builder.AppendLine("|---|---:|---:|---:|---:|");

        foreach (BenchmarkMetrics row in rows)
        {
            builder.Append("| ")
                .Append(row.EngineName)
                .Append(" | ")
                .Append(row.TotalExecutionTimeMs.ToString("F3", CultureInfo.InvariantCulture))
                .Append(" | ")
                .Append(row.P50LatencyMs.ToString("F6", CultureInfo.InvariantCulture))
                .Append(" | ")
                .Append(row.P99LatencyMs.ToString("F6", CultureInfo.InvariantCulture))
                .Append(" | ")
                .Append(row.TotalGcAllocatedMb.ToString("F3", CultureInfo.InvariantCulture))
                .AppendLine(" |");
        }

        return builder.ToString();
    }

    /// <summary>
    /// Emits the exact CSV schema required for downstream graphing:
    /// <c>Scenario,Engine,ExecutionTime_ms,P99Latency_ms,AllocatedMemory_MB</c>.
    /// </summary>
    public static string ToCsv(string scenarioLabel, IEnumerable<BenchmarkMetrics> rows)
    {
        var builder = new StringBuilder();
        builder.AppendLine("Scenario,Engine,ExecutionTime_ms,P99Latency_ms,AllocatedMemory_MB");

        foreach (BenchmarkMetrics row in rows)
        {
            builder
                .Append(scenarioLabel).Append(',')
                .Append(row.EngineName).Append(',')
                .Append(row.TotalExecutionTimeMs.ToString("F3", CultureInfo.InvariantCulture)).Append(',')
                .Append(row.P99LatencyMs.ToString("F6", CultureInfo.InvariantCulture)).Append(',')
                .Append(row.TotalGcAllocatedMb.ToString("F3", CultureInfo.InvariantCulture))
                .AppendLine();
        }

        return builder.ToString();
    }
}
