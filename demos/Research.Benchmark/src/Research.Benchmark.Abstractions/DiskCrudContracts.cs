namespace Research.Benchmark.Abstractions;

/// <summary>
/// A disk-durable CRUD benchmark engine (Scenario: <c>disk-crud-wal</c>): bulk-insert a batch, then
/// apply many point updates. Engines may choose whether the update phase is autocommit or an explicit
/// transaction batch; the benchmark host drives the phase boundary through <see cref="BeginUpdateBatch"/>
/// and <see cref="CompleteUpdateBatch"/>.
/// <para>
/// Reuses <see cref="FlatRecord"/> as the row shape. Inserts run inside a single batch/transaction
/// (one flush amortizes the bulk load); updates are driven one-by-one inside the configured update phase.
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

    /// <summary>Opens the update phase transaction/batch. Engines that intentionally use autocommit may no-op.</summary>
    void BeginUpdateBatch()
    {
    }

    /// <summary>Applies one durable point update (by primary key). Implementations must throw if no row was updated.</summary>
    void Update(long id, int newValue, double newScore);

    /// <summary>Commits the update phase transaction/batch opened by <see cref="BeginUpdateBatch"/>.</summary>
    void CompleteUpdateBatch()
    {
    }
}
