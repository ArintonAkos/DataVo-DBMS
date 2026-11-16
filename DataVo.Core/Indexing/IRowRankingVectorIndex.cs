namespace DataVo.Core.Indexing;

/// <summary>
/// A vector index that can rank a given set of rows by distance to a query, reading the vectors it stores.
/// A filtered query uses it when the rows passing the filter are found first and only they are ranked.
/// </summary>
internal interface IRowRankingVectorIndex
{
    /// <summary>
    /// Appends to <paramref name="nearest"/> up to <paramref name="topK"/> of <paramref name="rowIds"/>, nearest
    /// to <paramref name="queryVector"/> first. Rows the index holds no vector for are skipped.
    /// </summary>
    void RankNearest(ReadOnlySpan<float> queryVector, IReadOnlyList<RowKey> rowIds, int topK, List<RowKey> nearest);
}

/// <summary>Shared ranking step of <see cref="IRowRankingVectorIndex"/> implementations.</summary>
internal static class RowRanking
{
    /// <summary>
    /// Sorts the first <paramref name="count"/> rows by their distances and appends the nearest
    /// <paramref name="topK"/> to <paramref name="nearest"/>.
    /// </summary>
    public static void AppendNearest(float[] distances, RowKey[] rowIds, int count, int topK, List<RowKey> nearest)
    {
        Array.Sort(distances, rowIds, 0, count);
        int take = Math.Min(topK, count);
        for (int i = 0; i < take; i++)
        {
            nearest.Add(rowIds[i]);
        }
    }
}
