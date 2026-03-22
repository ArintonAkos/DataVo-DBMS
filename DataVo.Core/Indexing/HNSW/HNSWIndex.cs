namespace DataVo.Core.Indexing.HNSW;

/// <summary>
/// HNSW (Hierarchical Navigable Small World) index backend.
/// Implements approximate nearest-neighbor search for vector data.
/// </summary>
/// <remarks>
/// <para>
/// Current implementation uses a simple snapshot-based approach with cosine and euclidean distance metrics.
/// Future enhancements could include:
/// - True HNSW graph structure for better search efficiency
/// - Configurable M and ef parameters
/// - Multi-layer navigation
/// - Improved insertion/deletion algorithms
/// </para>
/// </remarks>
public class HNSWIndex : IIndexBase
{
    public string IndexType => "HNSW";

    /// <summary>
    /// Gets the distance metric used for this index ("cosine" or "euclidean").
    /// </summary>
    public string Metric { get; set; } = "cosine";

    /// <summary>
    /// Gets the vector entries: rowId -> float array.
    /// </summary>
    public Dictionary<long, float[]> Entries { get; set; } = [];

    /// <summary>
    /// Inserts or updates a vector entry.
    /// </summary>
    public void Insert(long rowId, float[] vector)
    {
        if (vector == null || vector.Length == 0)
            throw new ArgumentException("Vector cannot be null or empty", nameof(vector));

        Entries[rowId] = vector;
    }

    /// <summary>
    /// Deletes vector entries by row IDs.
    /// </summary>
    public void Delete(List<long> rowIds)
    {
        foreach (var rowId in rowIds)
            Entries.Remove(rowId);
    }

    /// <summary>
    /// Searches for the top-k nearest vectors.
    /// </summary>
    public List<long> SearchTopK(float[] queryVector, int topK)
    {
        if (topK <= 0)
            return [];

        if (queryVector == null || queryVector.Length == 0)
            throw new ArgumentException("Query vector cannot be null or empty", nameof(queryVector));

        if (Entries.Count == 0)
            return [];

        var ranked = new List<(long RowId, float Distance)>(Entries.Count);

        foreach (var entry in Entries)
        {
            if (entry.Value.Length != queryVector.Length)
                continue;

            float distance = Metric == "cosine"
                ? ComputeCosineDistance(queryVector, entry.Value)
                : ComputeEuclideanDistance(queryVector, entry.Value);

            ranked.Add((entry.Key, distance));
        }

        return ranked
            .OrderBy(item => item.Distance)
            .ThenBy(item => item.RowId)
            .Take(topK)
            .Select(item => item.RowId)
            .ToList();
    }

    /// <summary>
    /// Gets the count of vectors in this index.
    /// </summary>
    public int Count => Entries.Count;

    /// <summary>
    /// Clears all entries from this index.
    /// </summary>
    public void Clear()
    {
        Entries.Clear();
    }

    // Distance metrics (local implementations to avoid external dependencies)

    private static float ComputeCosineDistance(float[] a, float[] b)
    {
        float dotProduct = 0f;
        float magnitudeA = 0f;
        float magnitudeB = 0f;

        for (int i = 0; i < a.Length; i++)
        {
            dotProduct += a[i] * b[i];
            magnitudeA += a[i] * a[i];
            magnitudeB += b[i] * b[i];
        }

        magnitudeA = MathF.Sqrt(magnitudeA);
        magnitudeB = MathF.Sqrt(magnitudeB);

        if (magnitudeA == 0 || magnitudeB == 0)
            return 1f; // Maximum distance (no similarity)

        float similarity = dotProduct / (magnitudeA * magnitudeB);
        return 1f - similarity; // Convert to distance (0 = identical, 2 = opposite)
    }

    private static float ComputeEuclideanDistance(float[] a, float[] b)
    {
        float sum = 0f;
        for (int i = 0; i < a.Length; i++)
        {
            float diff = a[i] - b[i];
            sum += diff * diff;
        }

        return MathF.Sqrt(sum);
    }
}
