namespace DataVo.Core.Indexing.HNSW;

internal sealed class BrowserFallbackVectorIndex : IVectorIndex
{
    private readonly Dictionary<long, float[]> _vectors = [];

    public string IndexType => "HNSW";
    public string Metric { get; set; } = "cosine";

    public void Insert(long rowId, float[] vector)
    {
        if (vector == null || vector.Length == 0)
        {
            throw new ArgumentException("Vector cannot be null or empty", nameof(vector));
        }

        _vectors[rowId] = [.. vector];
    }

    public void Delete(List<long> rowIds)
    {
        if (rowIds == null)
        {
            return;
        }

        foreach (long rowId in rowIds)
        {
            _vectors.Remove(rowId);
        }
    }

    public List<long> SearchTopK(float[] queryVector, int topK)
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
                    ? ComputeEuclideanDistance(queryVector, entry.Value)
                    : ComputeCosineDistance(queryVector, entry.Value)
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

    internal List<(long RowId, float[] Vector)> ExportEntries()
    {
        return _vectors
            .Select(entry => (RowId: entry.Key, Vector: (float[])[.. entry.Value]))
            .ToList();
    }

    internal void ImportEntries(IEnumerable<(long RowId, float[] Vector)> entries)
    {
        _vectors.Clear();
        foreach (var entry in entries)
        {
            _vectors[entry.RowId] = [.. entry.Vector];
        }
    }

    private static double ComputeEuclideanDistance(float[] left, float[] right)
    {
        double sum = 0d;
        for (int i = 0; i < left.Length; i++)
        {
            double diff = left[i] - right[i];
            sum += diff * diff;
        }

        return Math.Sqrt(sum);
    }

    private static double ComputeCosineDistance(float[] left, float[] right)
    {
        double dot = 0d;
        double leftNorm = 0d;
        double rightNorm = 0d;

        for (int i = 0; i < left.Length; i++)
        {
            dot += left[i] * right[i];
            leftNorm += left[i] * left[i];
            rightNorm += right[i] * right[i];
        }

        if (leftNorm <= 0d || rightNorm <= 0d)
        {
            return 1d;
        }

        double cosineSimilarity = dot / (Math.Sqrt(leftNorm) * Math.Sqrt(rightNorm));
        return 1d - cosineSimilarity;
    }
}