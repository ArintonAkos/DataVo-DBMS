using DataVo.Core.MVCC;
using Xunit;

namespace DataVo.Tests.MVCC;

public class SnapshotVisibilityEvaluatorTests
{
    [Fact]
    public void IsVersionVisible_WithVisibleVersion_ReturnsTrue()
    {
        var snapshot = new TransactionSnapshot(snapshotTimestamp: 100, transactionId: 1);
        var version = new RowVersion(xmin: 50, xmax: 0);

        bool result = SnapshotVisibilityEvaluator.IsVersionVisible(version, snapshot);

        Assert.True(result);
    }

    [Fact]
    public void IsVersionVisible_WithFutureVersion_ReturnsFalse()
    {
        var snapshot = new TransactionSnapshot(snapshotTimestamp: 100, transactionId: 1);
        var version = new RowVersion(xmin: 150, xmax: 0);

        bool result = SnapshotVisibilityEvaluator.IsVersionVisible(version, snapshot);

        Assert.False(result);
    }

    [Fact]
    public void IsVersionVisible_WithDeletedVersion_ReturnsFalse()
    {
        var snapshot = new TransactionSnapshot(snapshotTimestamp: 100, transactionId: 1);
        var version = new RowVersion(xmin: 50, xmax: 80);

        bool result = SnapshotVisibilityEvaluator.IsVersionVisible(version, snapshot);

        Assert.False(result);
    }

    [Fact]
    public void IsRowVisible_WithNullVersion_ReturnsFalse()
    {
        var snapshot = new TransactionSnapshot(snapshotTimestamp: 100, transactionId: 1);

        bool result = SnapshotVisibilityEvaluator.IsRowVisible(null, snapshot);

        Assert.False(result);
    }

    [Fact]
    public void IsRowVisible_WithVisibleVersion_ReturnsTrue()
    {
        var snapshot = new TransactionSnapshot(snapshotTimestamp: 100, transactionId: 1);
        var version = new RowVersion(xmin: 50, xmax: 0);

        bool result = SnapshotVisibilityEvaluator.IsRowVisible(version, snapshot);

        Assert.True(result);
    }

    [Fact]
    public void CanUpdateRow_WithVisibleAndNotDeletedVersion_ReturnsTrue()
    {
        var snapshot = new TransactionSnapshot(snapshotTimestamp: 100, transactionId: 1);
        var version = new RowVersion(xmin: 50, xmax: 0);

        bool result = SnapshotVisibilityEvaluator.CanUpdateRow(version, snapshot);

        Assert.True(result);
    }

    [Fact]
    public void CanUpdateRow_WithDeletedVersion_ReturnsFalse()
    {
        var snapshot = new TransactionSnapshot(snapshotTimestamp: 100, transactionId: 1);
        var version = new RowVersion(xmin: 50, xmax: 80);

        bool result = SnapshotVisibilityEvaluator.CanUpdateRow(version, snapshot);

        Assert.False(result);
    }

    [Fact]
    public void CanUpdateRow_WithFutureVersion_ReturnsFalse()
    {
        var snapshot = new TransactionSnapshot(snapshotTimestamp: 100, transactionId: 1);
        var version = new RowVersion(xmin: 150, xmax: 0);

        bool result = SnapshotVisibilityEvaluator.CanUpdateRow(version, snapshot);

        Assert.False(result);
    }

    [Fact]
    public void CanDeleteRow_WithVisibleAndNotDeletedVersion_ReturnsTrue()
    {
        var snapshot = new TransactionSnapshot(snapshotTimestamp: 100, transactionId: 1);
        var version = new RowVersion(xmin: 50, xmax: 0);

        bool result = SnapshotVisibilityEvaluator.CanDeleteRow(version, snapshot);

        Assert.True(result);
    }

    [Fact]
    public void FilterVisibleRows_FiltersOnlyVisibleRows()
    {
        var manager = new VersionStorageManager();
        var snapshot = new TransactionSnapshot(snapshotTimestamp: 100, transactionId: 1);

        manager.AllocateVersion("testdb", "testtable", rowId: 1, xmin: 50);  // visible
        manager.AllocateVersion("testdb", "testtable", rowId: 2, xmin: 150); // future
        manager.AllocateVersion("testdb", "testtable", rowId: 3, xmin: 10);
        manager.MarkVersionObsolete("testdb", "testtable", rowId: 3, xmax: 80); // deleted

        var rowIds = new[] { 1L, 2L, 3L };
        var visible = SnapshotVisibilityEvaluator.FilterVisibleRows(
            rowIds, manager, "testdb", "testtable", snapshot);

        Assert.Single(visible);
        Assert.Equal(1, visible[0]);
    }

    [Fact]
    public void FilterVisibleRows_EmptyListWithNoRows_ReturnsEmpty()
    {
        var manager = new VersionStorageManager();
        var snapshot = new TransactionSnapshot(snapshotTimestamp: 100, transactionId: 1);

        var visible = SnapshotVisibilityEvaluator.FilterVisibleRows(
            Array.Empty<long>(), manager, "testdb", "testtable", snapshot);

        Assert.Empty(visible);
    }

    [Fact]
    public void IsVersionVisible_WithNullSnapshot_ThrowsArgumentNullException()
    {
        var version = new RowVersion(xmin: 50, xmax: 0);

        Assert.Throws<ArgumentNullException>(() =>
            SnapshotVisibilityEvaluator.IsVersionVisible(version, null!));
    }

    [Fact]
    public void FilterVisibleRows_WithNullVersionStorage_ThrowsArgumentNullException()
    {
        var snapshot = new TransactionSnapshot(snapshotTimestamp: 100, transactionId: 1);

        Assert.Throws<ArgumentNullException>(() =>
            SnapshotVisibilityEvaluator.FilterVisibleRows(
                Array.Empty<long>(), null!, "testdb", "testtable", snapshot));
    }
}
