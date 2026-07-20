namespace DataVo.Core.Indexing;

internal interface ISpanVectorIndex
{
    void Insert(long rowId, ReadOnlySpan<float> vector);
    List<long> SearchTopK(ReadOnlySpan<float> queryVector, int topK);
}
