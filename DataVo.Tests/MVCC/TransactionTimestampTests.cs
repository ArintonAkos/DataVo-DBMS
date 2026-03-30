using DataVo.Core.MVCC;
using DataVo.Core.Transactions;
using Xunit;

namespace DataVo.Tests.MVCC;

public class TransactionTimestampTests
{
    [Fact]
    public void TransactionIdAllocator_AllocateTransactionId_ReturnsIncreasingIds()
    {
        var allocator = new TransactionIdAllocator();

        long id1 = allocator.AllocateTransactionId();
        long id2 = allocator.AllocateTransactionId();
        long id3 = allocator.AllocateTransactionId();

        Assert.Equal(1, id1);
        Assert.Equal(2, id2);
        Assert.Equal(3, id3);
    }

    [Fact]
    public void TransactionIdAllocator_AllocateRange_ReturnsConsecutiveIds()
    {
        var allocator = new TransactionIdAllocator();

        var (start, end) = allocator.AllocateRange(5);

        Assert.Equal(1, start);
        Assert.Equal(5, end);

        long nextId = allocator.AllocateTransactionId();
        Assert.Equal(6, nextId);
    }

    [Fact]
    public void TransactionIdAllocator_GetCurrentHighWaterMark_ReturnsLastAllocatedId()
    {
        var allocator = new TransactionIdAllocator();

        allocator.AllocateTransactionId();
        allocator.AllocateTransactionId();
        allocator.AllocateTransactionId();

        long highWaterMark = allocator.GetCurrentHighWaterMark();
        Assert.Equal(3, highWaterMark);
    }

    [Fact]
    public void TransactionIdAllocator_Reset_ResetsAllocationCounter()
    {
        var allocator = new TransactionIdAllocator();

        allocator.AllocateTransactionId();
        allocator.AllocateTransactionId();
        allocator.Reset();

        long nextId = allocator.AllocateTransactionId();
        Assert.Equal(1, nextId);
    }

    [Fact]
    public void TransactionManager_BeginWithAllocator_AssignsTransactionIdAndSnapshot()
    {
        var allocator = new TransactionIdAllocator();
        var manager = new TransactionManager();
        var sessionId = Guid.NewGuid();

        manager.Begin(sessionId, allocator);

        var context = manager.GetContext(sessionId);
        Assert.NotNull(context);
        Assert.Equal(1, context.TransactionId);
        Assert.NotNull(context.Snapshot);
        Assert.Equal(1, context.Snapshot.TransactionId);
    }

    [Fact]
    public void TransactionManager_MultipleSessions_AssignDifferentTransactionIds()
    {
        var allocator = new TransactionIdAllocator();
        var manager = new TransactionManager();
        var session1 = Guid.NewGuid();
        var session2 = Guid.NewGuid();

        manager.Begin(session1, allocator);
        manager.Begin(session2, allocator);

        var context1 = manager.GetContext(session1);
        var context2 = manager.GetContext(session2);

        Assert.NotNull(context1);
        Assert.NotNull(context2);
        Assert.NotEqual(context1.TransactionId, context2.TransactionId);
        Assert.Equal(1, context1.TransactionId);
        Assert.Equal(2, context2.TransactionId);
    }

    [Fact]
    public void TransactionSnapshot_CreatedAtTime_IsRecorded()
    {
        var snapshot = new TransactionSnapshot(snapshotTimestamp: 100, transactionId: 1);

        Assert.NotEqual(DateTime.MinValue, snapshot.CreatedAt);
        Assert.True(snapshot.CreatedAt <= DateTime.UtcNow);
    }

    [Fact]
    public void TransactionContext_WithSnapshot_CanEvaluateVersionVisibility()
    {
        var context = new TransactionContext
        {
            TransactionId = 1,
            Snapshot = new TransactionSnapshot(snapshotTimestamp: 100, transactionId: 1)
        };

        var visibleVersion = new RowVersion(xmin: 50, xmax: 0);
        var invisibleVersion = new RowVersion(xmin: 150, xmax: 0);

        Assert.True(context.Snapshot.CanSee(visibleVersion));
        Assert.False(context.Snapshot.CanSee(invisibleVersion));
    }
}
