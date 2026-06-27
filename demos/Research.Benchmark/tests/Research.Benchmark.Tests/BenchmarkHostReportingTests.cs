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

    [Fact]
    public void FormatsMarkdownWithFlatCrudPhaseAllocationColumnsWhenPresent()
    {
        var rows = new[]
        {
            new BenchmarkMetrics("DataVo", 1000d, 0.1d, 0.9d, 12.5d, 11.5d, 1.0d)
        };

        string markdown = BenchmarkReportFormatter.ToMarkdown(rows);

        Assert.Contains("| Engine Name | Total Execution Time (ms) | P50 Latency (ms) | P99 Latency (ms) | Insert GC Allocated (MB) | Lookup GC Allocated (MB) | Total GC Allocated (MB) |", markdown);
        Assert.Contains("| DataVo | 1000.000 | 0.100000 | 0.900000 | 11.500 | 1.000 | 12.500 |", markdown);
    }

    [Fact]
    public void FormatsCsvWithOptionalPhaseAllocationColumns()
    {
        var rows = new[]
        {
            new BenchmarkMetrics("DataVo", 1000d, 0.1d, 0.9d, 12.5d, 11.5d, 1.0d),
            new BenchmarkMetrics("SQLite", double.NaN, double.NaN, double.NaN, double.NaN)
        };

        string csv = BenchmarkReportFormatter.ToCsv("Flat_CRUD", rows);

        Assert.Contains("Scenario,Engine,ExecutionTime_ms,P99Latency_ms,AllocatedMemory_MB,InsertAllocatedMemory_MB,LookupAllocatedMemory_MB", csv);
        Assert.Contains("Flat_CRUD,DataVo,1000.000,0.900000,12.500,11.500,1.000", csv);
        Assert.Contains("Flat_CRUD,SQLite,n/a,n/a,n/a,n/a,n/a", csv);
    }

    [Fact]
    public void FormatsMarkdownWithConcurrentOpsColumnsWhenPresent()
    {
        var rows = new[]
        {
            new BenchmarkMetrics(
                "DataVo",
                5000d,
                0d,
                0d,
                0d,
                OpsPerSecond: 120_000d,
                ReadP99LatencyMs: 0.25d,
                WriteP99LatencyMs: 2.5d)
        };

        string markdown = BenchmarkReportFormatter.ToMarkdown(rows);

        Assert.Contains("| Engine Name | Total Execution Time (ms) | OPS | Read P99 Latency (ms) | Write P99 Latency (ms) | Total GC Allocated (MB) |", markdown);
        Assert.Contains("| DataVo | 5000.000 | 120000.000 | 0.250000 | 2.500000 | 0.000 |", markdown);
    }

    [Fact]
    public void FormatsCsvWithConcurrentOpsColumnsWhenPresent()
    {
        var rows = new[]
        {
            new BenchmarkMetrics(
                "SQLite",
                5000d,
                0d,
                0d,
                1.5d,
                OpsPerSecond: 90_000d,
                ReadP99LatencyMs: 0.5d,
                WriteP99LatencyMs: 10.5d)
        };

        string csv = BenchmarkReportFormatter.ToCsv("Concurrent_Ops", rows);

        Assert.Contains("Scenario,Engine,ExecutionTime_ms,OPS,ReadP99Latency_ms,WriteP99Latency_ms,AllocatedMemory_MB", csv);
        Assert.Contains("Concurrent_Ops,SQLite,5000.000,90000.000,0.500000,10.500000,1.500", csv);
    }
}
