using Research.Benchmark.Host;

namespace Research.Benchmark.Tests;

public sealed class BenchmarkHostReportingTests
{
    [Fact]
    public void CalculatesNearestRankPercentilesFromPerIterationLatencies()
    {
        double[] latencies = Enumerable.Range(1, 100).Select(i => (double)i).ToArray();

        (double p50, double p99) = BenchmarkMetricsCalculator.CalculatePercentiles(latencies);

        Assert.Equal(50d, p50);
        Assert.Equal(99d, p99);
    }

    [Fact]
    public void FormatsFinalMarkdownTableWithoutPerIterationRows()
    {
        var rows = new[]
        {
            new BenchmarkMetrics("DataVo", 1000d, 0.1d, 0.9d, 12.5d),
            new BenchmarkMetrics("SQLite", 2000d, 0.2d, 1.9d, 25.5d)
        };

        string markdown = BenchmarkReportFormatter.ToMarkdown(rows);

        Assert.Contains("| Engine Name | Total Execution Time (ms) | P50 Latency (ms) | P99 Latency (ms) | Total GC Allocated (MB) |", markdown);
        Assert.Contains("| DataVo | 1000.000 | 0.100000 | 0.900000 | 12.500 |", markdown);
        Assert.Contains("| SQLite | 2000.000 | 0.200000 | 1.900000 | 25.500 |", markdown);
        Assert.DoesNotContain("Iteration", markdown);
    }
}
