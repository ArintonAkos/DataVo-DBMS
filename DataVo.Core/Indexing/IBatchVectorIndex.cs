namespace DataVo.Core.Indexing;

internal interface IBatchVectorIndex
{
    void InsertBatch(RowKey[] rowIds, float[] vectors, int vectorDimension);
}
