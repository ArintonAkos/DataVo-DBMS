namespace DataVo.Core.Indexing;

internal interface IReservableVectorIndex
{
    void Reserve(int expectedCount, int vectorDimension);
}
