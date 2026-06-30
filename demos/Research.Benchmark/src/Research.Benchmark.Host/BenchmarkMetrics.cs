using System.Globalization;
using System.Text;

namespace Research.Benchmark.Host;

public sealed record BenchmarkMetrics(
    string EngineName,
    double TotalExecutionTimeMs,
    double P50LatencyMs,
    double P99LatencyMs,
    double TotalGcAllocatedMb,
    double? InsertGcAllocatedMb = null,
    double? LookupGcAllocatedMb = null,
    double? OpsPerSecond = null,
    double? ReadP99LatencyMs = null,
    double? WriteP99LatencyMs = null,
    double? DiskSizeMb = null,
    double? RecoveryTimeMs = null);

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
        BenchmarkMetrics[] materializedRows = rows.ToArray();
        bool includeConcurrentOps = materializedRows.Any(row =>
            row.OpsPerSecond.HasValue || row.ReadP99LatencyMs.HasValue || row.WriteP99LatencyMs.HasValue);
        bool includeSpaceRecovery = materializedRows.Any(row =>
            row.DiskSizeMb.HasValue || row.RecoveryTimeMs.HasValue);
        bool includePhaseAllocations = materializedRows.Any(row =>
            row.InsertGcAllocatedMb.HasValue || row.LookupGcAllocatedMb.HasValue);

        var builder = new StringBuilder();
        if (includeSpaceRecovery)
        {
            builder.AppendLine("| Engine Name | Insert Execution Time (ms) | Disk Size (MB) | Recovery Time (ms) | Total GC Allocated (MB) |");
            builder.AppendLine("|---|---:|---:|---:|---:|");
        }
        else if (includeConcurrentOps)
        {
            builder.AppendLine("| Engine Name | Total Execution Time (ms) | OPS | Read P99 Latency (ms) | Write P99 Latency (ms) | Total GC Allocated (MB) |");
            builder.AppendLine("|---|---:|---:|---:|---:|---:|");
        }
        else if (includePhaseAllocations)
        {
            builder.AppendLine("| Engine Name | Total Execution Time (ms) | P50 Latency (ms) | P99 Latency (ms) | Insert GC Allocated (MB) | Lookup GC Allocated (MB) | Total GC Allocated (MB) |");
            builder.AppendLine("|---|---:|---:|---:|---:|---:|---:|");
        }
        else
        {
            builder.AppendLine("| Engine Name | Total Execution Time (ms) | P50 Latency (ms) | P99 Latency (ms) | Total GC Allocated (MB) |");
            builder.AppendLine("|---|---:|---:|---:|---:|");
        }

        foreach (BenchmarkMetrics row in materializedRows)
        {
            builder.Append("| ")
                .Append(row.EngineName)
                .Append(" | ")
                .Append(row.TotalExecutionTimeMs.ToString("F3", CultureInfo.InvariantCulture))
                .Append(" | ");

            if (includeSpaceRecovery)
            {
                builder
                    .Append(FormatOptional(row.DiskSizeMb, "F3"))
                    .Append(" | ")
                    .Append(FormatOptional(row.RecoveryTimeMs, "F3"))
                    .Append(" | ");
            }
            else if (includeConcurrentOps)
            {
                builder
                    .Append(FormatOptional(row.OpsPerSecond, "F3"))
                    .Append(" | ")
                    .Append(FormatOptional(row.ReadP99LatencyMs, "F6"))
                    .Append(" | ")
                    .Append(FormatOptional(row.WriteP99LatencyMs, "F6"))
                    .Append(" | ");
            }
            else
            {
                builder
                    .Append(row.P50LatencyMs.ToString("F6", CultureInfo.InvariantCulture))
                    .Append(" | ")
                    .Append(row.P99LatencyMs.ToString("F6", CultureInfo.InvariantCulture))
                    .Append(" | ");
            }

            if (!includeConcurrentOps && includePhaseAllocations)
            {
                builder
                    .Append(FormatOptionalMb(row.InsertGcAllocatedMb))
                    .Append(" | ")
                    .Append(FormatOptionalMb(row.LookupGcAllocatedMb))
                    .Append(" | ");
            }

            builder
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
        BenchmarkMetrics[] materializedRows = rows.ToArray();
        bool includeConcurrentOps = materializedRows.Any(row =>
            row.OpsPerSecond.HasValue || row.ReadP99LatencyMs.HasValue || row.WriteP99LatencyMs.HasValue);
        bool includeSpaceRecovery = materializedRows.Any(row =>
            row.DiskSizeMb.HasValue || row.RecoveryTimeMs.HasValue);

        var builder = new StringBuilder();
        if (includeSpaceRecovery)
        {
            builder.AppendLine("Scenario,Engine,InsertExecutionTime_ms,DiskSize_MB,RecoveryTime_ms,AllocatedMemory_MB");

            foreach (BenchmarkMetrics row in materializedRows)
            {
                bool unavailable = double.IsNaN(row.TotalExecutionTimeMs);
                builder
                    .Append(scenarioLabel).Append(',')
                    .Append(row.EngineName).Append(',')
                    .Append(unavailable ? "n/a" : row.TotalExecutionTimeMs.ToString("F3", CultureInfo.InvariantCulture)).Append(',')
                    .Append(unavailable ? "n/a" : FormatOptional(row.DiskSizeMb, "F3")).Append(',')
                    .Append(unavailable ? "n/a" : FormatOptional(row.RecoveryTimeMs, "F3")).Append(',')
                    .Append(unavailable ? "n/a" : row.TotalGcAllocatedMb.ToString("F3", CultureInfo.InvariantCulture))
                    .AppendLine();
            }

            return builder.ToString();
        }

        if (includeConcurrentOps)
        {
            builder.AppendLine("Scenario,Engine,ExecutionTime_ms,OPS,ReadP99Latency_ms,WriteP99Latency_ms,AllocatedMemory_MB");

            foreach (BenchmarkMetrics row in materializedRows)
            {
                bool unavailable = double.IsNaN(row.TotalExecutionTimeMs);
                builder
                    .Append(scenarioLabel).Append(',')
                    .Append(row.EngineName).Append(',')
                    .Append(unavailable ? "n/a" : row.TotalExecutionTimeMs.ToString("F3", CultureInfo.InvariantCulture)).Append(',')
                    .Append(unavailable ? "n/a" : FormatOptional(row.OpsPerSecond, "F3")).Append(',')
                    .Append(unavailable ? "n/a" : FormatOptional(row.ReadP99LatencyMs, "F6")).Append(',')
                    .Append(unavailable ? "n/a" : FormatOptional(row.WriteP99LatencyMs, "F6")).Append(',')
                    .Append(unavailable ? "n/a" : row.TotalGcAllocatedMb.ToString("F3", CultureInfo.InvariantCulture))
                    .AppendLine();
            }

            return builder.ToString();
        }

        builder.AppendLine("Scenario,Engine,ExecutionTime_ms,P99Latency_ms,AllocatedMemory_MB,InsertAllocatedMemory_MB,LookupAllocatedMemory_MB");

        foreach (BenchmarkMetrics row in materializedRows)
        {
            // A NaN total marks an engine that could not run in this environment (e.g. a missing native
            // extension); report it as n/a rather than a fabricated number.
            bool unavailable = double.IsNaN(row.TotalExecutionTimeMs);
            builder
                .Append(scenarioLabel).Append(',')
                .Append(row.EngineName).Append(',')
                .Append(unavailable ? "n/a" : row.TotalExecutionTimeMs.ToString("F3", CultureInfo.InvariantCulture)).Append(',')
                .Append(unavailable ? "n/a" : row.P99LatencyMs.ToString("F6", CultureInfo.InvariantCulture)).Append(',')
                .Append(unavailable ? "n/a" : row.TotalGcAllocatedMb.ToString("F3", CultureInfo.InvariantCulture)).Append(',')
                .Append(unavailable ? "n/a" : FormatOptionalCsvMb(row.InsertGcAllocatedMb)).Append(',')
                .Append(unavailable ? "n/a" : FormatOptionalCsvMb(row.LookupGcAllocatedMb))
                .AppendLine();
        }

        return builder.ToString();
    }

    private static string FormatOptionalMb(double? value) =>
        value.HasValue ? value.Value.ToString("F3", CultureInfo.InvariantCulture) : "n/a";

    private static string FormatOptionalCsvMb(double? value) =>
        value.HasValue ? value.Value.ToString("F3", CultureInfo.InvariantCulture) : "n/a";

    private static string FormatOptional(double? value, string format) =>
        value.HasValue ? value.Value.ToString(format, CultureInfo.InvariantCulture) : "n/a";
}
