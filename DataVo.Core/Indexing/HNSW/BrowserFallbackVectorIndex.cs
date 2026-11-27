using DataVo.Core.Utils;

namespace DataVo.Core.Indexing.HNSW;

internal sealed class BrowserFallbackVectorIndex : IVectorIndex
{
    private readonly Dictionary<RowKey, float[]> _vectors = [];

    public string IndexType => "HNSW";
    public string Metric { get; set; } = "cosine";

    public void Insert(RowKey rowId, float[] vector)
    {
        if (vector == null || vector.Length == 0)
        {
            throw new ArgumentException("Vector cannot be null or empty", nameof(vector));
        }

        _vectors[rowId] = [.. vector];
    }

    public void Delete(List<RowKey> rowIds)
    {
        if (rowIds == null)
        {
            return;
        }

        foreach (RowKey rowId in rowIds)
        {
            _vectors.Remove(rowId);
        }
    }

    public List<RowKey> SearchTopK(float[] queryVector, int topK)
    {
        if (queryVector == null || queryVector.Length == 0 || topK <= 0 || _vectors.Count == 0)
        {
            return [];
        }

        bool useEuclidean = Metric.Equals("l2", StringComparison.OrdinalIgnoreCase)
            || Metric.Equals("euclidean", StringComparison.OrdinalIgnoreCase);

        return _vectors
            .Where(entry => entry.Value.Length == queryVector.Length)
            .Select(entry => new
            {
                RowId = entry.Key,
                Distance = useEuclidean
                    ? SimdDistanceKernels.EuclideanDistanceDouble(queryVector, entry.Value)
                    : SimdDistanceKernels.CosineDistanceDouble(queryVector, entry.Value)
            })
            .OrderBy(candidate => candidate.Distance)
            .Take(topK)
            .Select(candidate => candidate.RowId)
            .ToList();
    }

    public void Clear()
    {
        _vectors.Clear();
    }
}