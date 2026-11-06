namespace DataVo.Core.Indexing;

internal interface ISpanVectorIndex
{
    void Insert(RowKey rowId, ReadOnlySpan<float> vector);
    List<RowKey> SearchTopK(ReadOnlySpan<float> queryVector, int topK);
}
