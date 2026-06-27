namespace Research.Benchmark.Abstractions;

/// <summary>Options for the concurrent read/write contention benchmark.</summary>
public sealed record ConcurrentOpsOptions(
    int InitialRecords,
    TimeSpan Duration,
    int ReaderWorkers,
    int WriterWorkers,
    TimeSpan BusyTimeout)
{
    public static ConcurrentOpsOptions Default { get; } = new(
        InitialRecords: 100_000,
        Duration: TimeSpan.FromSeconds(5),
        ReaderWorkers: 8,
        WriterWorkers: 2,
        BusyTimeout: TimeSpan.FromSeconds(3));
}

/// <summary>Measured output for a concurrent read/write benchmark run.</summary>
public sealed record ConcurrentOpsResult(
    long ReadOperations,
    long WriteOperations,
    double TotalOperationsPerSecond,
    double ReadP99LatencyMs,
    double WriteP99LatencyMs,
    double ElapsedMs);

/// <summary>
/// A concurrent operations benchmark engine. Implementations preload flat records, then run many read and
/// write workers against the same logical database for a fixed duration.
/// </summary>
public interface IConcurrentOpsEngine : IAsyncDisposable
{
    string Name { get; }

    Task InitializeAsync(ConcurrentOpsOptions options, CancellationToken cancellationToken = default);

    Task<ConcurrentOpsResult> RunAsync(ConcurrentOpsOptions options, CancellationToken cancellationToken = default);
}
