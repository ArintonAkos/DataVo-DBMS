using System.Globalization;

namespace DataVo.Core.Indexing.Flat;

/// <summary>
/// Factory for exact flat vector index instances.
/// </summary>
public sealed class FlatVectorIndexFactory : IVectorIndexFactory
{
    /// <summary>
    /// Gets the index type identifier handled by this factory.
    /// </summary>
    public string IndexType => "FLAT";

    /// <summary>
    /// Creates a flat vector index instance.
    /// </summary>
    public object CreateIndex(string indexName, string columnName, Dictionary<string, object> @params)
    {
        var index = new FlatVectorIndex();
        if (@params.TryGetValue("metric", out object? rawMetric) && rawMetric is string metric)
        {
            index.Metric = NormalizeMetric(metric);
        }

        if (TryReadInt(@params, "expectedCount", out int expectedCount)
            && TryReadInt(@params, "dimension", out int dimension)
            && expectedCount > 0
            && dimension > 0)
        {
            index.Reserve(expectedCount, dimension);
        }

        return index;
    }

    /// <summary>
    /// Loads an existing flat index instance via the configured persistence handler.
    /// </summary>
    public object LoadIndex(string filePath, IIndexPersistence persistence)
    {
        return persistence.LoadIndex(filePath);
    }

    private static string NormalizeMetric(string metric)
    {
        return metric.ToLower(CultureInfo.InvariantCulture) switch
        {
            "l2" or "euclidean" => "euclidean",
            _ => "cosine"
        };
    }

    private static bool TryReadInt(Dictionary<string, object> parameters, string key, out int value)
    {
        value = 0;
        if (!parameters.TryGetValue(key, out object? raw) || raw is null)
        {
            return false;
        }

        return raw switch
        {
            int i => Assign(i, out value),
            long l when l is >= int.MinValue and <= int.MaxValue => Assign((int)l, out value),
            string s => int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out value),
            _ => false
        };

        static bool Assign(int source, out int target)
        {
            target = source;
            return true;
        }
    }
}
