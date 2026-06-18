namespace DataVo.Core.StorageEngine.Memory;

internal interface IInMemoryStorageSnapshotProvider
{
    InMemoryStorageSnapshot CreateSnapshot();

    void RestoreSnapshot(InMemoryStorageSnapshot snapshot);
}
