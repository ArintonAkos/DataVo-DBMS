namespace Research.Benchmark.Abstractions;

/// <summary>
/// A vector-search benchmark engine: insert high-dimensional vectors, then Top-K nearest-neighbour query.
/// Synchronous so per-op <c>Task</c> allocations don't distort <c>AllocatedMemory_MB</c>; all in-memory.
/// </summary>
public interface IVectorSearchEngine : IDisposable
{
    /// <summary>The engine display name used in benchmark output.</summary>
    string Name { get; }

    /// <summary>
    /// Prepares an empty in-memory store for vectors of the given dimensionality. Implementations that
    /// cannot run in this environment (e.g. a missing native extension) should throw here so the host can
    /// mark the engine n/a rather than report a fabricated number.
    /// </summary>
    void Initialize(int dimensions);

    /// <summary>Begins a bulk-insert batch (a single transaction for engines that auto-commit per write).</summary>
    void BeginBatch();

    /// <summary>Commits the bulk-insert batch (and builds the index if the engine builds it post-load).</summary>
    void CompleteBatch();

    /// <summary>Inserts one vector with its id.</summary>
    void Insert(long id, float[] vector);

    /// <summary>Returns the ids of the <paramref name="k"/> nearest vectors to <paramref name="query"/>.</summary>
    IReadOnlyList<long> Search(float[] query, int k);
}
