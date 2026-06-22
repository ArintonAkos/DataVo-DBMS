namespace Research.Benchmark.Abstractions;

/// <summary>A nested address on an order (Scenario B deep-document workload).</summary>
public sealed record OrderAddress(string Kind, string Street, string City, string PostalCode);

/// <summary>A nested line item on an order.</summary>
public sealed record OrderItem(int Sku, string Name, int Quantity, double UnitPrice);

/// <summary>A deeply nested order: header + line items + addresses.</summary>
public sealed record DeepOrder(
    long Id,
    string Customer,
    double Total,
    IReadOnlyList<OrderItem> Items,
    IReadOnlyList<OrderAddress> Addresses);

/// <summary>
/// A deep-document benchmark engine: save a nested order, then load it whole by id. Synchronous so per-op
/// <c>Task</c> allocations don't distort <c>AllocatedMemory_MB</c>; all implementations run in-memory.
/// </summary>
public interface IDeepDocumentEngine : IDisposable
{
    /// <summary>The engine display name used in benchmark output.</summary>
    string Name { get; }

    /// <summary>Creates an empty in-memory store ready for saves.</summary>
    void Initialize();

    /// <summary>Begins a bulk-save batch (a single transaction for engines that auto-commit per write).</summary>
    void BeginBatch();

    /// <summary>Commits/flushes the bulk-save batch.</summary>
    void CompleteBatch();

    /// <summary>Persists one order with all of its nested children.</summary>
    void Save(DeepOrder order);

    /// <summary>Loads and fully reconstructs an order (header + items + addresses) by id.</summary>
    DeepOrder? Load(long id);
}
