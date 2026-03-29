using DataVo.Core.Transactions;
using DataVo.Core.Exceptions;
using System.Reflection;

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
        var start = new ManualResetEventSlim(false);
        var firstAcquired = new ManualResetEventSlim(false);
        var secondAcquired = new ManualResetEventSlim(false);

        Task first = Task.Run(() =>
        {
            start.Wait();
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
            start.Wait();
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

        start.Set();

        bool bothAcquired = firstAcquired.Wait(10000) && secondAcquired.Wait(10000);
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

        Assert.True(writerAcquired.Wait(3000), "Writer should acquire after reader releases lock.");
        Assert.True(writerDone.Wait(3000), "Writer should finish after acquiring and releasing row write lock.");

        await writer;
    }

    [Fact]
    public void RowLocks_AreCleanedUp_WhenNoLongerInUse()
    {
        var locks = new LockManager();
        const string db = "db";
        const string table = "users";

        for (int i = 1; i <= 32; i++)
        {
            locks.AcquireRowReadLock(db, table, i);
            locks.ReleaseRowReadLock(db, table, i);
        }

        Assert.Equal(0, GetPrivateRowLockCount(locks));
    }

    [Fact]
    public void TableLocks_AreCleanedUp_WhenNoLongerInUse()
    {
        var locks = new LockManager();
        const string db = "db";
        const string table = "users";

        locks.AcquireReadLock(db, table);
        locks.ReleaseReadLock(db, table);

        locks.AcquireWriteLock(db, table);
        locks.ReleaseWriteLock(db, table);

        Assert.Equal(0, GetPrivateTableLockCount(locks));
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
    public async Task RowWriteLock_TimesOut_WhenHeldByReader()
    {
        var locks = new LockManager(lockAcquireTimeoutMs: 50);
        const string db = "db";
        const string table = "users";
        const long rowId = 7;

        var readerReady = new ManualResetEventSlim(false);
        var releaseReader = new ManualResetEventSlim(false);

        Task reader = Task.Run(() =>
        {
            locks.AcquireRowReadLock(db, table, rowId);
            readerReady.Set();
            try
            {
                releaseReader.Wait();
            }
            finally
            {
                locks.ReleaseRowReadLock(db, table, rowId);
            }
        });

        Assert.True(readerReady.Wait(3000), "Reader should acquire row lock before writer attempts.");
        try
        {
            TimeoutException ex = Assert.Throws<TimeoutException>(() =>
                locks.AcquireRowWriteLock(db, table, rowId));

            Assert.Contains("Timed out acquiring row write lock", ex.Message);
        }
        finally
        {
            releaseReader.Set();
            await reader;
        }
    }

    [Fact]
    public async Task TableWriteLock_TimesOut_WhenHeldByReader()
    {
        var locks = new LockManager(lockAcquireTimeoutMs: 50);
        const string db = "db";
        const string table = "users";

        var readerReady = new ManualResetEventSlim(false);
        var releaseReader = new ManualResetEventSlim(false);

        Task reader = Task.Run(() =>
        {
            locks.AcquireReadLock(db, table);
            readerReady.Set();
            try
            {
                releaseReader.Wait();
            }
            finally
            {
                locks.ReleaseReadLock(db, table);
            }
        });

        Assert.True(readerReady.Wait(3000), "Reader should acquire table lock before writer attempts.");
        try
        {
            TimeoutException ex = Assert.Throws<TimeoutException>(() =>
                locks.AcquireWriteLock(db, table));

            Assert.Contains("Timed out acquiring table write lock", ex.Message);
        }
        finally
        {
            releaseReader.Set();
            await reader;
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

    [Fact]
    public async Task OpposingRowWriteLocks_DetectDeadlock_WithCycleDiagnostics()
    {
        var locks = new LockManager(lockAcquireTimeoutMs: 1000);
        const string db = "db";
        const string table = "users";

        var ready = new CountdownEvent(2);
        var startSecondAcquire = new ManualResetEventSlim(false);
        Exception? firstError = null;
        Exception? secondError = null;

        Task first = Task.Run(() =>
        {
            bool acquiredSecond = false;
            locks.AcquireRowWriteLock(db, table, 1);
            try
            {
                ready.Signal();
                startSecondAcquire.Wait();
                locks.AcquireRowWriteLock(db, table, 2);
                acquiredSecond = true;
            }
            catch (Exception ex)
            {
                firstError = ex;
            }
            finally
            {
                if (acquiredSecond)
                {
                    locks.ReleaseRowWriteLock(db, table, 2);
                }

                locks.ReleaseRowWriteLock(db, table, 1);
            }
        });

        Task second = Task.Run(() =>
        {
            bool acquiredSecond = false;
            locks.AcquireRowWriteLock(db, table, 2);
            try
            {
                ready.Signal();
                startSecondAcquire.Wait();
                locks.AcquireRowWriteLock(db, table, 1);
                acquiredSecond = true;
            }
            catch (Exception ex)
            {
                secondError = ex;
            }
            finally
            {
                if (acquiredSecond)
                {
                    locks.ReleaseRowWriteLock(db, table, 1);
                }

                locks.ReleaseRowWriteLock(db, table, 2);
            }
        });

        Assert.True(ready.Wait(3000), "Both workers should acquire their first row lock.");
        startSecondAcquire.Set();
        await Task.WhenAll(first, second);

        List<Exception> errors = new Exception?[] { firstError, secondError }
            .Where(ex => ex is not null)
            .Select(ex => ex!)
            .ToList();

        DeadlockDetectedException deadlock = Assert.Single(errors.OfType<DeadlockDetectedException>());
        Assert.Contains("Wait-for cycle", deadlock.Message);
        Assert.Contains("#row:", deadlock.Message);
    }

    [Fact]
    public async Task OpposingTableWriteLocks_DetectDeadlock_WithCycleDiagnostics()
    {
        var locks = new LockManager(lockAcquireTimeoutMs: 1000);
        const string db = "db";
        const string firstTable = "users";
        const string secondTable = "orders";

        var ready = new CountdownEvent(2);
        var startSecondAcquire = new ManualResetEventSlim(false);
        Exception? firstError = null;
        Exception? secondError = null;

        Task first = Task.Run(() =>
        {
            bool acquiredSecond = false;
            locks.AcquireWriteLock(db, firstTable);
            try
            {
                ready.Signal();
                startSecondAcquire.Wait();
                locks.AcquireWriteLock(db, secondTable);
                acquiredSecond = true;
            }
            catch (Exception ex)
            {
                firstError = ex;
            }
            finally
            {
                if (acquiredSecond)
                {
                    locks.ReleaseWriteLock(db, secondTable);
                }

                locks.ReleaseWriteLock(db, firstTable);
            }
        });

        Task second = Task.Run(() =>
        {
            bool acquiredSecond = false;
            locks.AcquireWriteLock(db, secondTable);
            try
            {
                ready.Signal();
                startSecondAcquire.Wait();
                locks.AcquireWriteLock(db, firstTable);
                acquiredSecond = true;
            }
            catch (Exception ex)
            {
                secondError = ex;
            }
            finally
            {
                if (acquiredSecond)
                {
                    locks.ReleaseWriteLock(db, firstTable);
                }

                locks.ReleaseWriteLock(db, secondTable);
            }
        });

        Assert.True(ready.Wait(3000), "Both workers should acquire their first table lock.");
        startSecondAcquire.Set();
        await Task.WhenAll(first, second);

        List<Exception> errors = new Exception?[] { firstError, secondError }
            .Where(ex => ex is not null)
            .Select(ex => ex!)
            .ToList();

        DeadlockDetectedException deadlock = Assert.Single(errors.OfType<DeadlockDetectedException>());
        Assert.Contains("Wait-for cycle", deadlock.Message);
        Assert.Contains($"{db}.{firstTable}", deadlock.Message);
    }

    private static int GetPrivateRowLockCount(LockManager locks)
    {
        var field = typeof(LockManager).GetField("_rowLocks", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);

        object? value = field!.GetValue(locks);
        Assert.NotNull(value);

        var countProperty = value!.GetType().GetProperty("Count");
        Assert.NotNull(countProperty);

        object? count = countProperty!.GetValue(value);
        Assert.NotNull(count);

        return (int)count!;
    }

    private static int GetPrivateTableLockCount(LockManager locks)
    {
        var field = typeof(LockManager).GetField("_tableLocks", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);

        object? value = field!.GetValue(locks);
        Assert.NotNull(value);

        var countProperty = value!.GetType().GetProperty("Count");
        Assert.NotNull(countProperty);

        object? count = countProperty!.GetValue(value);
        Assert.NotNull(count);

        return (int)count!;
    }
}
