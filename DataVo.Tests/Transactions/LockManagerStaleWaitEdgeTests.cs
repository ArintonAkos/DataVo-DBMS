using DataVo.Core;
using DataVo.Core.Exceptions;
using DataVo.Core.Transactions;

namespace DataVo.Tests.Transactions;

/// <summary>A waiter whose blocker has already released must not count toward a deadlock; a real cycle still must.</summary>
public class LockManagerStaleWaitEdgeTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    [Fact]
    public void ReleasedBlocker_IsNotReportedAsADeadlock()
    {
        var locks = new LockManager();
        using var writerHoldsTwo = new ManualResetEventSlim();
        using var readerHoldsOne = new ManualResetEventSlim();
        using var readerIsWaiting = new ManualResetEventSlim();
        using var letReaderContinue = new ManualResetEventSlim();
        int hookCalls = 0;

        // Holds the reader after its wait on row 2 is recorded, before it can be woken: a descheduled thread.
        locks.WaitRegisteredForTesting = () =>
        {
            if (Interlocked.Increment(ref hookCalls) == 1)
            {
                readerIsWaiting.Set();
                Assert.True(letReaderContinue.Wait(Timeout));
            }
        };

        var reader = Task.Factory.StartNew(() =>
        {
            locks.AcquireRowReadLock("D", "T", 1);
            readerHoldsOne.Set();
            Assert.True(writerHoldsTwo.Wait(Timeout));
            locks.AcquireRowReadLock("D", "T", 2);
            locks.ReleaseRowReadLock("D", "T", 2);
            locks.ReleaseRowReadLock("D", "T", 1);
        }, TaskCreationOptions.LongRunning);

        var writer = Task.Factory.StartNew(() =>
        {
            Assert.True(readerHoldsOne.Wait(Timeout));
            locks.AcquireRowWriteLock("D", "T", 2);
            writerHoldsTwo.Set();
            Assert.True(readerIsWaiting.Wait(Timeout));
            locks.ReleaseRowWriteLock("D", "T", 2);

            // The reader's edge to this thread is stale now, and row 1 is held by the reader.
            letReaderContinueAfterBlocking();
            locks.AcquireRowWriteLock("D", "T", 1);
            locks.ReleaseRowWriteLock("D", "T", 1);
        }, TaskCreationOptions.LongRunning);

        Assert.True(Task.WaitAll([reader, writer], Timeout));

        void letReaderContinueAfterBlocking()
        {
            locks.WaitRegisteredForTesting = letReaderContinue.Set;
        }
    }

    [Fact]
    public void RealCycle_IsStillReported()
    {
        var locks = new LockManager();
        using var bothHoldOne = new Barrier(2);
        DeadlockDetectedException? detected = null;

        void Run(RowKey first, RowKey second)
        {
            locks.AcquireRowWriteLock("D", "T", first);
            bothHoldOne.SignalAndWait(Timeout);
            try
            {
                locks.AcquireRowWriteLock("D", "T", second);
                locks.ReleaseRowWriteLock("D", "T", second);
            }
            catch (DeadlockDetectedException exception)
            {
                detected = exception;
            }
            finally
            {
                locks.ReleaseRowWriteLock("D", "T", first);
            }
        }

        Task a = Task.Factory.StartNew(() => Run(1, 2), TaskCreationOptions.LongRunning);
        Task b = Task.Factory.StartNew(() => Run(2, 1), TaskCreationOptions.LongRunning);

        Assert.True(Task.WaitAll([a, b], Timeout));
        Assert.NotNull(detected);
    }
}
