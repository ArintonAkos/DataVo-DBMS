namespace DataVo.Core.Indexing;

internal interface IBatchVectorIndex
{
    void InsertBatch(long[] rowIds, float[] vectors, int vectorDimension);
}
