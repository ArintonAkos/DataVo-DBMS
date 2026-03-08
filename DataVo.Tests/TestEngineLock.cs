namespace DataVo.Tests;

/// <summary>
/// Provides a single process-wide lock for tests that mutate the shared storage singleton,
/// catalog state, or disk-backed test directories.
/// </summary>
internal static class TestEngineLock
{
    /// <summary>
    /// Gets the shared semaphore used to serialize storage-engine test setup and teardown.
    /// </summary>
    internal static SemaphoreSlim Instance { get; } = new(1, 1);
}
