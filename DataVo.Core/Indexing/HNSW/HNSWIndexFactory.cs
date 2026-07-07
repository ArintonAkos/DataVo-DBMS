namespace DataVo.Core.Indexing.HNSW;

using System.Globalization;

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
        object index = IsBrowserRuntime()
            ? new BrowserFallbackVectorIndex()
            : new HNSWIndex();

        // Extract metric parameter (cosine or euclidean)
        if (@params.TryGetValue("metric", out var val) && val is string metric)
        {
            string normalizedMetric = NormalizeMetric(metric);
            if (index is HNSWIndex hnswWithMetric)
                hnswWithMetric.Metric = normalizedMetric;

            if (index is BrowserFallbackVectorIndex fallbackWithMetric)
                fallbackWithMetric.Metric = normalizedMetric;
        }

        // Extract vector dimension for validation (future use)
        if (@params.TryGetValue("dimension", out var dimVal) && dimVal is int dimension)
        {
            // Could validate or store dimension when inserting vectors
        }

        if (index is HNSWIndex hnsw && TryReadInt(@params, "m", out int m) && m > 0)
            hnsw.M = m;

        if (index is HNSWIndex hnswEfConstruction && TryReadInt(@params, "efConstruction", out int efConstruction) && efConstruction > 0)
            hnswEfConstruction.EfConstruction = efConstruction;

        if (index is HNSWIndex hnswAdaptiveConstruction && TryReadBool(@params, "enableAdaptiveEfConstruction", out bool enableAdaptiveEfConstruction))
            hnswAdaptiveConstruction.EnableAdaptiveEfConstruction = enableAdaptiveEfConstruction;

        if (index is HNSWIndex hnswAdaptiveMultiplier && TryReadDouble(@params, "adaptiveEfConstructionMultiplier", out double adaptiveEfConstructionMultiplier) && adaptiveEfConstructionMultiplier > 0d)
            hnswAdaptiveMultiplier.AdaptiveEfConstructionMultiplier = adaptiveEfConstructionMultiplier;

        if (index is HNSWIndex hnswMaxAdaptiveConstruction && TryReadInt(@params, "maxAdaptiveEfConstruction", out int maxAdaptiveEfConstruction))
            hnswMaxAdaptiveConstruction.MaxAdaptiveEfConstruction = maxAdaptiveEfConstruction;

        if (index is HNSWIndex hnswInsertionExpansion && TryReadBool(@params, "enableInsertionCandidateExpansion", out bool enableInsertionCandidateExpansion))
            hnswInsertionExpansion.EnableInsertionCandidateExpansion = enableInsertionCandidateExpansion;

        if (index is HNSWIndex hnswInsertionFactor && TryReadDouble(@params, "insertionCandidateExpansionFactor", out double insertionCandidateExpansionFactor) && insertionCandidateExpansionFactor > 0d)
            hnswInsertionFactor.InsertionCandidateExpansionFactor = insertionCandidateExpansionFactor;

        if (index is HNSWIndex hnswAdaptiveInsertion && TryReadBool(@params, "enableAdaptiveInsertionCandidateExpansion", out bool enableAdaptiveInsertionCandidateExpansion))
            hnswAdaptiveInsertion.EnableAdaptiveInsertionCandidateExpansion = enableAdaptiveInsertionCandidateExpansion;

        if (index is HNSWIndex hnswAdaptiveInsertionMin && TryReadDouble(@params, "adaptiveInsertionExpansionMinFactor", out double adaptiveInsertionExpansionMinFactor) && adaptiveInsertionExpansionMinFactor > 0d)
            hnswAdaptiveInsertionMin.AdaptiveInsertionExpansionMinFactor = adaptiveInsertionExpansionMinFactor;

        if (index is HNSWIndex hnswAdaptiveInsertionMax && TryReadDouble(@params, "adaptiveInsertionExpansionMaxFactor", out double adaptiveInsertionExpansionMaxFactor) && adaptiveInsertionExpansionMaxFactor > 0d)
            hnswAdaptiveInsertionMax.AdaptiveInsertionExpansionMaxFactor = adaptiveInsertionExpansionMaxFactor;

        if (index is HNSWIndex hnswPruning && TryReadBool(@params, "enableInsertionNeighborhoodPruning", out bool enableInsertionNeighborhoodPruning))
            hnswPruning.EnableInsertionNeighborhoodPruning = enableInsertionNeighborhoodPruning;

        if (index is HNSWIndex hnswPruningThreshold && TryReadDouble(@params, "insertionNeighborhoodPruningThreshold", out double insertionNeighborhoodPruningThreshold) && insertionNeighborhoodPruningThreshold > 0d)
            hnswPruningThreshold.InsertionNeighborhoodPruningThreshold = insertionNeighborhoodPruningThreshold;

        if (index is HNSWIndex hnswPruneHops && TryReadInt(@params, "insertionNeighborhoodPruneHops", out int insertionNeighborhoodPruneHops) && insertionNeighborhoodPruneHops > 0)
            hnswPruneHops.InsertionNeighborhoodPruneHops = insertionNeighborhoodPruneHops;

        if (index is HNSWIndex hnswEfSearch && TryReadInt(@params, "efSearch", out int efSearch) && efSearch > 0)
            hnswEfSearch.EfSearch = efSearch;

        if (index is HNSWIndex hnswDiversity && TryReadBool(@params, "enableDiversityHeuristic", out bool enableDiversityHeuristic))
            hnswDiversity.EnableDiversityHeuristic = enableDiversityHeuristic;

        if (index is HNSWIndex hnswDeleteRepair && TryReadBool(@params, "enableDeleteGraphRepair", out bool enableDeleteGraphRepair))
            hnswDeleteRepair.EnableDeleteGraphRepair = enableDeleteGraphRepair;

        if (index is HNSWIndex hnswAdaptiveSearch && TryReadBool(@params, "enableAdaptiveEfSearch", out bool enableAdaptiveEfSearch))
            hnswAdaptiveSearch.EnableAdaptiveEfSearch = enableAdaptiveEfSearch;

        if (index is HNSWIndex hnswAdaptiveSearchMultiplier && TryReadDouble(@params, "adaptiveEfSearchMultiplier", out double adaptiveEfSearchMultiplier) && adaptiveEfSearchMultiplier > 0d)
            hnswAdaptiveSearchMultiplier.AdaptiveEfSearchMultiplier = adaptiveEfSearchMultiplier;

        return index;
    }

    private static bool IsBrowserRuntime()
    {
        return BrowserRuntimeFlags.ForceVectorFallback;
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
            string s => int.TryParse(
                s.AsSpan(),
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out value),
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
            string s => double.TryParse(
                s.AsSpan(),
                NumberStyles.Float | NumberStyles.AllowThousands,
                CultureInfo.InvariantCulture,
                out value),
            _ => false
        };

        static bool Assign(double source, out double target)
        {
            target = source;
            return true;
        }
    }
}
