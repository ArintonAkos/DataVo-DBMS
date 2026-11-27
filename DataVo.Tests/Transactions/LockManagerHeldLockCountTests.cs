using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.Transactions;

namespace DataVo.Tests.Transactions;

/// <summary>
/// The per-thread count of held locks (which decides whether a writer may bypass the snapshot gate's turn order)
/// matches the locks the thread really holds: re-entry is refused without counting, every release — immediate or
/// deferred to an LSM batch's publication — uncounts, and statements leave nothing counted behind.
/// </summary>
public sealed class LockManagerHeldLockCountTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_heldlocks_{Guid.NewGuid():N}");

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    [Fact]
    public void ReenteringAnOwnedWriteLock_IsRefusedAndNotCounted()
    {
        OnOwnThread(() =>
        {
            var locks = new LockManager(lockAcquireTimeoutMs: 200);
            Assert.False(LockManager.CurrentThreadHoldsLocks);

            locks.AcquireWriteLock("Db", "T");
            locks.AcquireRowWriteLock("Db", "T", (RowKey)5);
            Assert.True(LockManager.CurrentThreadHoldsLocks);

            Assert.ThrowsAny<Exception>(() => locks.AcquireWriteLock("Db", "T"));
            Assert.ThrowsAny<Exception>(() => locks.AcquireRowWriteLock("Db", "T", (RowKey)5));

            locks.ReleaseRowWriteLock("Db", "T", (RowKey)5);
            Assert.True(LockManager.CurrentThreadHoldsLocks);
            locks.ReleaseWriteLock("Db", "T");
            Assert.False(LockManager.CurrentThreadHoldsLocks);

            locks.AcquireReadLock("Db", "T");
            locks.AcquireRowReadLock("Db", "T", (RowKey)7);
            locks.ReleaseRowReadLock("Db", "T", (RowKey)7);
            locks.ReleaseReadLock("Db", "T");
            Assert.False(LockManager.CurrentThreadHoldsLocks);
        });
    }

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public void Statements_LeaveNoLockCounted_IncludingDeferredReleases(StorageMode mode)
    {
        OnOwnThread(() =>
        {
            using var db = new DataVoContext(new DataVoConfig
            {
                StorageMode = mode,
                DataDirectory = mode == StorageMode.InMemory ? null! : _directory,
                LsmStrictFsync = false,
            });
            Run(db, "CREATE DATABASE H");
            Run(db, "USE H");
            Run(db, "CREATE TABLE T (Id INT PRIMARY KEY, V INT)");

            // On LSM, write-lock releases are deferred to the batch's publication and re-acquired virtually.
            foreach (string sql in new[]
            {
                "INSERT INTO T (Id, V) VALUES (1, 1), (2, 2)",
                "UPDATE T SET V = V + 1 WHERE Id <= 2",
                "DELETE FROM T WHERE Id = 2",
                "BEGIN",
                "UPDATE T SET V = 9 WHERE Id = 1",
                "INSERT INTO T (Id, V) VALUES (3, 3)",
                "COMMIT",
                "SELECT * FROM T",
            })
            {
                Run(db, sql);
                Assert.False(LockManager.CurrentThreadHoldsLocks, $"{sql} left a lock counted");
            }

            Assert.True(db.Execute("INSERT INTO T (Id, V) VALUES (1, 0)")[^1].IsError);
            Assert.False(LockManager.CurrentThreadHoldsLocks, "a failed statement left a lock counted");
            Assert.Equal(0, db.Engine.CommitClock.InFlightCount);
        });
    }

    private static void Run(DataVoContext db, string sql)
    {
        QueryResult result = db.Execute(sql)[^1];
        Assert.False(result.IsError, $"{sql} => {string.Join(" | ", result.Messages)}");
    }

    /// <summary>Runs on a fresh thread: the count is per thread, and a test-runner thread may carry another test's state.</summary>
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
        Assert.True(thread.Join(TimeSpan.FromSeconds(60)));
        if (failure is not null)
        {
            throw failure;
        }
    }
}
