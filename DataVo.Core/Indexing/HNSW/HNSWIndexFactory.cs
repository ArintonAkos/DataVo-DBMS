namespace DataVo.Core.Indexing.HNSW;

/// <summary>
/// Factory for creating HNSW (vector) index instances.
/// </summary>
public class HNSWIndexFactory : IVectorIndexFactory
{
    /// <summary>
    /// Gets the index type identifier handled by this factory.
    /// </summary>
    public string IndexType => "HNSW";

    /// <summary>
    /// Creates a new HNSW index instance.
    /// </summary>
    public object CreateIndex(string indexName, string columnName, Dictionary<string, object> @params)
    {
        var index = new HNSWIndex();

        // Extract metric parameter (cosine or euclidean)
        if (@params.TryGetValue("metric", out var val) && val is string metric)
            index.Metric = NormalizeMetric(metric);

        // Extract vector dimension for validation (future use)
        if (@params.TryGetValue("dimension", out var dimVal) && dimVal is int dimension)
        {
            // Could validate or store dimension when inserting vectors
        }

        return index;
    }

    /// <summary>
    /// Loads an existing HNSW index instance via the configured persistence handler.
    /// </summary>
    public object LoadIndex(string filePath, IIndexPersistence persistence)
    {
        // Delegate to persistence handler
        return persistence.LoadIndex(filePath);
    }

    private static string NormalizeMetric(string metric)
    {
        return metric.ToLowerInvariant() switch
        {
            "cosine" => "cosine",
            "l2" or "euclidean" => "euclidean",
            _ => "cosine" // Default
        };
    }
}
