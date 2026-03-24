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

        if (TryReadInt(@params, "m", out int m) && m > 0)
            index.M = m;

        if (TryReadInt(@params, "efConstruction", out int efConstruction) && efConstruction > 0)
            index.EfConstruction = efConstruction;

        if (TryReadBool(@params, "enableAdaptiveEfConstruction", out bool enableAdaptiveEfConstruction))
            index.EnableAdaptiveEfConstruction = enableAdaptiveEfConstruction;

        if (TryReadDouble(@params, "adaptiveEfConstructionMultiplier", out double adaptiveEfConstructionMultiplier) && adaptiveEfConstructionMultiplier > 0d)
            index.AdaptiveEfConstructionMultiplier = adaptiveEfConstructionMultiplier;

        if (TryReadInt(@params, "efSearch", out int efSearch) && efSearch > 0)
            index.EfSearch = efSearch;

        if (TryReadBool(@params, "enableDiversityHeuristic", out bool enableDiversityHeuristic))
            index.EnableDiversityHeuristic = enableDiversityHeuristic;

        if (TryReadBool(@params, "enableDeleteGraphRepair", out bool enableDeleteGraphRepair))
            index.EnableDeleteGraphRepair = enableDeleteGraphRepair;

        if (TryReadBool(@params, "enableAdaptiveEfSearch", out bool enableAdaptiveEfSearch))
            index.EnableAdaptiveEfSearch = enableAdaptiveEfSearch;

        if (TryReadDouble(@params, "adaptiveEfSearchMultiplier", out double adaptiveEfSearchMultiplier) && adaptiveEfSearchMultiplier > 0d)
            index.AdaptiveEfSearchMultiplier = adaptiveEfSearchMultiplier;

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

    private static bool TryReadInt(Dictionary<string, object> parameters, string key, out int value)
    {
        value = 0;

        if (!parameters.TryGetValue(key, out var raw) || raw is null)
            return false;

        return raw switch
        {
            int i => Assign(i, out value),
            long l when l is >= int.MinValue and <= int.MaxValue => Assign((int)l, out value),
            double d when d is >= int.MinValue and <= int.MaxValue => Assign((int)d, out value),
            float f when f is >= int.MinValue and <= int.MaxValue => Assign((int)f, out value),
            string s => int.TryParse(s, out value),
            _ => false
        };

        static bool Assign(int source, out int target)
        {
            target = source;
            return true;
        }
    }

    private static bool TryReadBool(Dictionary<string, object> parameters, string key, out bool value)
    {
        value = false;

        if (!parameters.TryGetValue(key, out var raw) || raw is null)
            return false;

        return raw switch
        {
            bool b => Assign(b, out value),
            string s => bool.TryParse(s, out value),
            _ => false
        };

        static bool Assign(bool source, out bool target)
        {
            target = source;
            return true;
        }
    }

    private static bool TryReadDouble(Dictionary<string, object> parameters, string key, out double value)
    {
        value = 0d;

        if (!parameters.TryGetValue(key, out var raw) || raw is null)
            return false;

        return raw switch
        {
            double d => Assign(d, out value),
            float f => Assign(f, out value),
            int i => Assign(i, out value),
            long l => Assign(l, out value),
            string s => double.TryParse(s, out value),
            _ => false
        };

        static bool Assign(double source, out double target)
        {
            target = source;
            return true;
        }
    }
}
