using DataVo.Core;
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
    public void IsVersionVisible_WithNullSnapshot_ThrowsArgumentNullException()
    {
        var version = new RowVersion(xmin: 50, xmax: 0);

        Assert.Throws<ArgumentNullException>(() =>
            SnapshotVisibilityEvaluator.IsVersionVisible(version, null!));
    }
}
