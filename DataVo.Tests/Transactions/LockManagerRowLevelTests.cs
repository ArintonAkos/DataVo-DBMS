using DataVo.Core.Transactions;

namespace DataVo.Tests.Transactions;

public class LockManagerRowLevelTests
{
    [Fact]
    public async Task RowWriteLocks_DifferentRows_DoNotSerialize()
    {
        var locks = new LockManager();
        const string db = "db";
        const string table = "users";

        var gate = new ManualResetEventSlim(false);
        var firstAcquired = new ManualResetEventSlim(false);
        var secondAcquired = new ManualResetEventSlim(false);

        Task first = Task.Run(() =>
        {
            locks.AcquireRowWriteLock(db, table, 1);
            try
            {
                firstAcquired.Set();
                gate.Wait();
            }
            finally
            {
                locks.ReleaseRowWriteLock(db, table, 1);
            }
        });

        Task second = Task.Run(() =>
        {
            locks.AcquireRowWriteLock(db, table, 2);
            try
            {
                secondAcquired.Set();
                gate.Wait();
            }
            finally
            {
                locks.ReleaseRowWriteLock(db, table, 2);
            }
        });

        bool bothAcquired = firstAcquired.Wait(250) && secondAcquired.Wait(250);
        Assert.True(bothAcquired, "Expected both disjoint row write locks to be acquired without serialization.");

        gate.Set();
        await Task.WhenAll(first, second);
    }

    [Fact]
    public async Task RowWriteLock_BlocksBehindRowReadLock_OnSameRow()
    {
        var locks = new LockManager();
        const string db = "db";
        const string table = "users";
        const long rowId = 42;

        var writerAcquired = new ManualResetEventSlim(false);
        var writerDone = new ManualResetEventSlim(false);

        locks.AcquireRowReadLock(db, table, rowId);

        Task writer = Task.Run(() =>
        {
            locks.AcquireRowWriteLock(db, table, rowId);
            try
            {
                writerAcquired.Set();
            }
            finally
            {
                locks.ReleaseRowWriteLock(db, table, rowId);
                writerDone.Set();
            }
        });

        Thread.Sleep(60);
        Assert.False(writerAcquired.IsSet, "Writer should be blocked while row read lock is held.");

        locks.ReleaseRowReadLock(db, table, rowId);

        Assert.True(writerAcquired.Wait(1000), "Writer should acquire after reader releases lock.");
        Assert.True(writerDone.Wait(1000), "Writer should finish after acquiring and releasing row write lock.");

        await writer;
    }

    [Fact]
    public void AcquireRowReadLocks_ReturnsDeterministicOrderedDistinctIds()
    {
        var locks = new LockManager();
        const string db = "db";
        const string table = "users";

        List<long> acquired = locks.AcquireRowReadLocks(db, table, [5, 2, 2, 9, 5, 3]);
        try
        {
            Assert.Equal([2, 3, 5, 9], acquired);
        }
        finally
        {
            locks.ReleaseRowReadLocks(db, table, acquired);
        }
    }

    [Fact]
    public async Task AcquireRowWriteLocks_OpposingOrders_AvoidsDeadlock()
    {
        var locks = new LockManager();
        const string db = "db";
        const string table = "users";

        var start = new ManualResetEventSlim(false);

        Task first = Task.Run(() =>
        {
            start.Wait();
            List<long> acquired = locks.AcquireRowWriteLocks(db, table, new long[] { 2, 1 });
            try
            {
                Thread.Sleep(40);
            }
            finally
            {
                locks.ReleaseRowWriteLocks(db, table, acquired);
            }
        });

        Task second = Task.Run(() =>
        {
            start.Wait();
            List<long> acquired = locks.AcquireRowWriteLocks(db, table, new long[] { 1, 2 });
            try
            {
                Thread.Sleep(40);
            }
            finally
            {
                locks.ReleaseRowWriteLocks(db, table, acquired);
            }
        });

        start.Set();

        Task all = Task.WhenAll(first, second);
        Task completed = await Task.WhenAny(all, Task.Delay(1500));

        Assert.Same(all, completed);
        await all;
    }
}
