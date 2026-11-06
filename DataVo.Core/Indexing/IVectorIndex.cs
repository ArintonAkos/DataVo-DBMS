namespace DataVo.Core.Indexing;

/// <summary>
/// Capability contract for vector-search index implementations.
/// </summary>
public interface IVectorIndex : IIndexBase
{
    /// <summary>
    /// Inserts or updates a vector by row identifier.
    /// </summary>
    void Insert(RowKey rowId, float[] vector);

    /// <summary>
    /// Deletes vectors for the provided row identifiers.
    /// </summary>
    void Delete(List<RowKey> rowIds);

    /// <summary>
    /// Returns the nearest row identifiers for the query vector.
    /// </summary>
    List<RowKey> SearchTopK(float[] queryVector, int topK);

    /// <summary>
    /// Clears all entries from the index.
    /// </summary>
    void Clear();
}