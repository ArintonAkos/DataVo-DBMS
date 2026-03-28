using DataVo.Core.MVCC;
using Xunit;

namespace DataVo.Tests.MVCC;

public class RowVersionMetadataTests
{
    [Fact]
    public void RowVersion_CreatedWithXmin_HasCorrectValues()
    {
        var version = new RowVersion(xmin: 1, xmax: 0, versionChain: 0);

        Assert.Equal(1, version.Xmin);
        Assert.Equal(0, version.Xmax);
        Assert.Equal(0, version.VersionChain);
    }

    [Fact]
    public void RowVersion_DefaultConstructor_InitializesAllFieldsToZero()
    {
        var version = new RowVersion();

        Assert.Equal(0, version.Xmin);
        Assert.Equal(0, version.Xmax);
        Assert.Equal(0, version.VersionChain);
    }

    [Fact]
    public void TransactionSnapshot_CanSee_ReturnsTrueForVisibleVersion()
    {
        var txId = 1L;
        var snapshot = new TransactionSnapshot(snapshotTimestamp: 100, transactionId: txId);
        var version = new RowVersion(xmin: 50, xmax: 0);

        Assert.True(snapshot.CanSee(version));
    }

    [Fact]
    public void TransactionSnapshot_CanSee_ReturnsFalseForFutureVersion()
    {
        var snapshot = new TransactionSnapshot(snapshotTimestamp: 100, transactionId: 1);
        var version = new RowVersion(xmin: 150, xmax: 0);

        Assert.False(snapshot.CanSee(version));
    }

    [Fact]
    public void TransactionSnapshot_CanSee_ReturnsFalseForDeletedVersion()
    {
        var snapshot = new TransactionSnapshot(snapshotTimestamp: 100, transactionId: 1);
        var version = new RowVersion(xmin: 50, xmax: 80);

        Assert.False(snapshot.CanSee(version));
    }

    [Fact]
    public void VersionStorageManager_AllocateVersion_StoresAndRetrievesVersion()
    {
        var manager = new VersionStorageManager();
        var version = manager.AllocateVersion("testdb", "testtable", rowId: 1, xmin: 42);

        Assert.Equal(42, version.Xmin);
        Assert.Equal(0, version.Xmax);

        var retrieved = manager.GetVersion("testdb", "testtable", rowId: 1);
        Assert.NotNull(retrieved);
        Assert.Equal(42, retrieved.Value.Xmin);
    }

    [Fact]
    public void VersionStorageManager_MarkVersionObsolete_UpdatesXmax()
    {
        var manager = new VersionStorageManager();
        manager.AllocateVersion("testdb", "testtable", rowId: 1, xmin: 42);

        manager.MarkVersionObsolete("testdb", "testtable", rowId: 1, xmax: 100);

        var version = manager.GetVersion("testdb", "testtable", rowId: 1);
        Assert.NotNull(version);
        Assert.Equal(100, version.Value.Xmax);
    }

    [Fact]
    public void VersionStorageManager_LinkVersionChain_ConnectsVersions()
    {
        var manager = new VersionStorageManager();
        manager.AllocateVersion("testdb", "testtable", rowId: 1, xmin: 42);
        manager.AllocateVersion("testdb", "testtable", rowId: 2, xmin: 100);

        manager.LinkVersionChain("testdb", "testtable", oldRowId: 1, newRowId: 2);

        var version1 = manager.GetVersion("testdb", "testtable", rowId: 1);
        Assert.NotNull(version1);
        Assert.Equal(2, version1.Value.VersionChain);
    }

    [Fact]
    public void VersionStorageManager_GetVersionChain_ReturnsAllVersionsInOrder()
    {
        var manager = new VersionStorageManager();
        manager.AllocateVersion("testdb", "testtable", rowId: 1, xmin: 10);
        manager.AllocateVersion("testdb", "testtable", rowId: 2, xmin: 20);
        manager.AllocateVersion("testdb", "testtable", rowId: 3, xmin: 30);

        manager.LinkVersionChain("testdb", "testtable", oldRowId: 1, newRowId: 2);
        manager.LinkVersionChain("testdb", "testtable", oldRowId: 2, newRowId: 3);

        var chain = manager.GetVersionChain("testdb", "testtable", startRowId: 1);

        Assert.Equal(3, chain.Count);
        Assert.Equal(1, chain[0].RowId);
        Assert.Equal(2, chain[1].RowId);
        Assert.Equal(3, chain[2].RowId);
    }

    [Fact]
    public void VersionStorageManager_ClearTableVersions_RemovesAllTableVersions()
    {
        var manager = new VersionStorageManager();
        manager.AllocateVersion("testdb", "testtable", rowId: 1, xmin: 42);
        manager.AllocateVersion("testdb", "testtable", rowId: 2, xmin: 50);

        manager.ClearTableVersions("testdb", "testtable");

        Assert.Null(manager.GetVersion("testdb", "testtable", rowId: 1));
        Assert.Null(manager.GetVersion("testdb", "testtable", rowId: 2));
    }
}
