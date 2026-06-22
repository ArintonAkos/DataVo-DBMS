namespace Research.Benchmark.Abstractions;

/// <summary>A flat record for the high-throughput CRUD benchmark (Scenario A).</summary>
public sealed record FlatRecord(long Id, string Name, int Value, double Score);

/// <summary>
/// A flat-CRUD benchmark engine: bulk insert then point-lookup by id. The interface is intentionally
/// <b>synchronous</b> (not async) so that per-operation <c>Task</c>/<c>ValueTask</c> allocations do not
/// distort the measured <c>AllocatedMemory_MB</c> — every implementation runs fully in-memory.
/// </summary>
public interface IFlatCrudEngine : IDisposable
{
    /// <summary>The engine display name used in benchmark output.</summary>
    string Name { get; }

    /// <summary>Creates an empty in-memory store ready for inserts.</summary>
    void Initialize();

    /// <summary>
    /// Begins a bulk-insert batch. Engines that auto-commit per write (e.g. LiteDB) should open a single
    /// transaction here so the insert phase measures CPU/serialization throughput, not per-write commit
    /// overhead. No-op for engines that don't need it.
    /// </summary>
    void BeginBatch();

    /// <summary>Commits/flushes the bulk-insert batch opened by <see cref="BeginBatch"/>.</summary>
    void CompleteBatch();

    /// <summary>Inserts a single record (within the active batch, if one is open).</summary>
    void Insert(FlatRecord record);

    /// <summary>Point-lookup by primary key; returns <c>null</c> when absent.</summary>
    FlatRecord? GetById(long id);
}
