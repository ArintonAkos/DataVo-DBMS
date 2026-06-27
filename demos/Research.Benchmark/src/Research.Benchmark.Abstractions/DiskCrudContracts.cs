namespace Research.Benchmark.Abstractions;

/// <summary>
/// A disk-durable CRUD benchmark engine (Scenario: <c>disk-crud-wal</c>): bulk-insert a batch, then
/// apply many individual point updates where each update is its own durable, autocommit write — the
/// regime where physical disk flushes (WAL append / fsync) dominate latency.
/// <para>
/// Reuses <see cref="FlatRecord"/> as the row shape. Inserts run inside a single batch/transaction
/// (one flush amortizes the bulk load); updates run one-by-one so per-write durability cost is exposed.
/// The interface is synchronous so per-op <c>Task</c>/<c>ValueTask</c> allocations don't distort the
/// measured allocation figures.
/// </para>
/// </summary>
public interface IDiskCrudEngine : IDisposable
{
    /// <summary>The engine display name used in benchmark output.</summary>
    string Name { get; }

    /// <summary>Creates the on-disk database (rooted at <paramref name="workingDirectory"/>) ready for inserts.</summary>
    void Initialize(string workingDirectory);

    /// <summary>Opens a single bulk-insert batch/transaction so the insert phase measures load throughput, not per-write commit cost.</summary>
    void BeginInsertBatch();

    /// <summary>Commits/flushes the bulk-insert batch opened by <see cref="BeginInsertBatch"/>.</summary>
    void CompleteInsertBatch();

    /// <summary>Inserts a single record into the active batch.</summary>
    void Insert(FlatRecord record);

    /// <summary>Applies one durable point update (by primary key). Implementations must throw if no row was updated.</summary>
    void Update(long id, int newValue, double newScore);
}
