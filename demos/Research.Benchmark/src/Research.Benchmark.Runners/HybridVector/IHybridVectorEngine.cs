namespace Research.Benchmark.Runners.HybridVector;

/// <summary>An engine answering filtered nearest-neighbour queries through its own SQL.</summary>
public interface IHybridVectorEngine : IDisposable
{
    string Name { get; }

    /// <summary>Storage, index and query form used, printed under the results.</summary>
    string Notes { get; }

    void Initialize(string directory, int dimensions);

    void Load(HybridVectorData data);

    /// <summary>The SQL text the engine runs for a shape.</summary>
    string SqlFor(HybridShape shape);

    /// <summary>Runs one query and returns the document ids in the engine's order (at most ten).</summary>
    IReadOnlyList<long> Search(HybridShape shape, float[] query, int queryIndex);

    /// <summary>
    /// Runs one query with the engine's planner diagnostics on and describes the route it took, or returns
    /// <see langword="null"/> when the engine has no such diagnostics. Never called while timing.
    /// </summary>
    HybridRouting? Explain(HybridShape shape, float[] query, int queryIndex);
}

/// <summary>DataVo's planner routing of one hybrid query.</summary>
/// <param name="Buckets">Hybrid-routing counters the query incremented (e.g. <c>hybrid.orderby.initial_topk.adaptive</c>).</param>
public sealed record HybridRouting(bool VectorIndexUsed, int VectorTopK, int ExpansionPasses, bool FullTableScan, IReadOnlyList<string> Buckets);
