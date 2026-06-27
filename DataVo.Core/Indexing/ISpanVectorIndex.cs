namespace DataVo.Core.Indexing;

internal interface ISpanVectorIndex
{
    void Insert(long rowId, ReadOnlySpan<float> vector);
}
