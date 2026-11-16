using System.Buffers;

namespace DataVo.Core.Indexing.HNSW;

/// <summary>Ranks given rows by exact distance, for queries that filter rows before they search.</summary>
public partial class HNSWIndex : IRowRankingVectorIndex
{
    /// <inheritdoc />
    void IRowRankingVectorIndex.RankNearest(ReadOnlySpan<float> queryVector, IReadOnlyList<RowKey> rowIds, int topK, List<RowKey> nearest)
    {
        if (topK <= 0 || rowIds.Count == 0 || queryVector.Length != _vectorDimension)
        {
            return;
        }

        float[] distances = ArrayPool<float>.Shared.Rent(rowIds.Count);
        RowKey[] ranked = ArrayPool<RowKey>.Shared.Rent(rowIds.Count);
        try
        {
            int count = 0;
            float queryInvNorm = ComputeInverseNorm(queryVector);

            // The row map changes under this gate; the stored vectors of a mapped, active row are stable.
            lock (_stateGate)
            {
                for (int i = 0; i < rowIds.Count; i++)
                {
                    if (_rowIdToOrdinal.TryGetValue(rowIds[i], out int ordinal) && _isActive[ordinal])
                    {
                        distances[count] = DistanceToOrdinal(queryVector, queryInvNorm, ordinal);
                        ranked[count++] = rowIds[i];
                    }
                }
            }

            RowRanking.AppendNearest(distances, ranked, count, topK, nearest);
        }
        finally
        {
            ArrayPool<float>.Shared.Return(distances);
            ArrayPool<RowKey>.Shared.Return(ranked);
        }
    }
}
