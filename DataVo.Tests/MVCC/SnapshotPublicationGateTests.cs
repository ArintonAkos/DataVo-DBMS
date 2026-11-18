using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.MVCC;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.MVCC;

/// <summary>
/// The snapshot gate never waits forever: an entry that cannot get in before the timeout fails with
/// <see cref="TimeoutException"/>, and the gate's waiting counts stay exact, so a timed-out waiter never keeps blocking
/// the other side afterwards.
/// </summary>
public sealed class SnapshotPublicationGateTests
{
    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void ReaderBehindAStalledWriter_TimesOut_AndEntersOnceItLeaves()
    {
        var gate = new SnapshotPublicationGate(waitTimeoutMs: 150);
        using var release = new ManualResetEventSlim();
        Task holder = HoldOnOwnThread(gate, write: true, release);

        Assert.Throws<TimeoutException>(() => OnOwnThread(gate.EnterRead));

        release.Set();
        Assert.True(holder.Wait(Deadline));
        OnOwnThread(() =>
        {
            gate.EnterRead();
            gate.Exit();
        });
    }

    [Fact]
    public void WriterBehindAStalledReader_TimesOut_AndEntersOnceItLeaves()
    {
        var gate = new SnapshotPublicationGate(waitTimeoutMs: 150);
        using var release = new ManualResetEventSlim();
        Task holder = HoldOnOwnThread(gate, write: false, release);

        Assert.Throws<TimeoutException>(() => OnOwnThread(() => gate.EnterWrite(holdsLocks: false)));

        release.Set();
        Assert.True(holder.Wait(Deadline));
        OnOwnThread(() =>
        {
            gate.EnterWrite(holdsLocks: false);
            gate.Exit();
        });
    }

    [Fact]
    public void TimedOutWriter_DoesNotBlockLaterReaders()
    {
        var gate = new SnapshotPublicationGate(waitTimeoutMs: 150);
        using var release = new ManualResetEventSlim();
        Task reader = HoldOnOwnThread(gate, write: false, release);

        // A waiting writer turns away new readers; once it gives up, readers must get in at once again.
        Assert.Throws<TimeoutException>(() => OnOwnThread(() => gate.EnterWrite(holdsLocks: false)));

        var entered = System.Diagnostics.Stopwatch.StartNew();
        OnOwnThread(() =>
        {
            gate.EnterRead();
            gate.Exit();
        });
        Assert.True(entered.ElapsedMilliseconds < 140, $"a later reader waited {entered.ElapsedMilliseconds} ms behind a writer that had timed out");

        release.Set();
        Assert.True(reader.Wait(Deadline));
    }

    [Fact]
    public void TimedOutReader_DoesNotBlockLaterWriters()
    {
        var gate = new SnapshotPublicationGate(waitTimeoutMs: 150);
        using var release = new ManualResetEventSlim();
        Task writer = HoldOnOwnThread(gate, write: true, release);

        // The waiting reader took the next turn; when it gives up, writers must not wait for it.
        Assert.Throws<TimeoutException>(() => OnOwnThread(gate.EnterRead));

        var entered = System.Diagnostics.Stopwatch.StartNew();
        OnOwnThread(() =>
        {
            gate.EnterWrite(holdsLocks: false);
            gate.Exit();
        });
        Assert.True(entered.ElapsedMilliseconds < 140, $"a later writer waited {entered.ElapsedMilliseconds} ms behind a reader that had timed out");

        release.Set();
        Assert.True(writer.Wait(Deadline));
    }

    [Fact]
    public void SnapshotStatement_BehindAStalledVersionWriter_FailsCleanly_AndTheTransactionGoesOn()
    {
        using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory, LockAcquireTimeoutMs = 200 });
        Guid tx = Guid.NewGuid();
        Ok(db, "CREATE DATABASE G", tx);
        Ok(db, "USE G", tx);
        Ok(db, "CREATE TABLE T (Id INT PRIMARY KEY, V INT)", tx);
        Ok(db, "INSERT INTO T (Id, V) VALUES (1, 10)", tx);
        Ok(db, "BEGIN", tx);

        using var release = new ManualResetEventSlim();
        Task stalled = HoldOnOwnThread(db.Engine.CommitClock.Gate, write: true, release);

        QueryResult blocked = db.Execute("SELECT V FROM T", tx)[^1];
        Assert.True(blocked.IsError);
        Assert.Contains(blocked.Messages, message => message.Contains("Timed out", StringComparison.Ordinal));

        release.Set();
        Assert.True(stalled.Wait(Deadline));
        Assert.Equal(10, Assert.Single(Ok(db, "SELECT V FROM T", tx).Data)["V"]);
        Ok(db, "UPDATE T SET V = 11 WHERE Id = 1", tx);
        Ok(db, "COMMIT", tx);
        Assert.Equal(0, db.Engine.CommitClock.InFlightCount);
    }

    /// <summary>Enters the gate on a dedicated thread (entries are per thread) and holds it until released.</summary>
    private static Task HoldOnOwnThread(SnapshotPublicationGate gate, bool write, ManualResetEventSlim release)
    {
        using var entered = new ManualResetEventSlim();
        Task task = Task.Factory.StartNew(() =>
        {
            if (write)
            {
                gate.EnterWrite(holdsLocks: false);
            }
            else
            {
                gate.EnterRead();
            }

            entered.Set();
            release.Wait();
            gate.Exit();
        }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        Assert.True(entered.Wait(Deadline));
        return task;
    }

    private static void OnOwnThread(Action action)
    {
        Exception? failure = null;
        var thread = new Thread(() =>
        {
            try
            {
                action();
            }
            catch (Exception ex)
            {
                failure = ex;
            }
        });
        thread.Start();
        Assert.True(thread.Join(Deadline));
        if (failure is not null)
        {
            throw failure;
        }
    }

    private static QueryResult Ok(DataVoContext db, string sql, Guid session)
    {
        QueryResult result = db.Execute(sql, session)[^1];
        Assert.False(result.IsError, $"{sql} => {string.Join(" | ", result.Messages)}");
        return result;
    }
}
