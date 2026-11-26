namespace DataVo.Tests.Indexing;

/// <summary>
/// A <see cref="FactAttribute"/> for long HNSW performance benchmarks. They run only when
/// <c>DATAVO_RUN_HNSW_PERF_TESTS</c> is <c>1</c> or <c>true</c>; otherwise the runner reports them as skipped
/// rather than as passed.
/// </summary>
public sealed class HnswPerformanceFactAttribute : FactAttribute
{
    /// <summary>The environment variable that opts in to the HNSW performance benchmarks.</summary>
    public const string EnvironmentVariable = "DATAVO_RUN_HNSW_PERF_TESTS";

    /// <summary>Initializes the attribute and skips the test unless the opt-in variable is set.</summary>
    public HnswPerformanceFactAttribute()
    {
        if (!IsEnabled())
        {
            Skip = $"HNSW performance benchmark: set {EnvironmentVariable}=1 to run it.";
        }
    }

    private static bool IsEnabled()
    {
        string? value = Environment.GetEnvironmentVariable(EnvironmentVariable);
        return string.Equals(value, "1", StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);
    }
}
