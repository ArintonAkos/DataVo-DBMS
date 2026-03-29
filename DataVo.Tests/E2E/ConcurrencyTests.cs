using DataVo.Core.Contracts.Results;
using DataVo.Core.Exceptions;
using DataVo.Core.Parser;
using DataVo.Core.Runtime;
using DataVo.Core.StorageEngine.Config;
using DataVo.Tests.BrowserParity;

namespace DataVo.Tests.E2E;

public abstract class ConcurrencyTestsBase(DataVoConfig config, string testDbName) : SqlExecutionTestsBase(config, testDbName)
{
    [Fact]
    [BrowserTranslateIgnore("Lock-manager concurrency orchestration test; not expressible as a single browser SQL scenario")]
    public async Task ConcurrentSelects_OnSameTable_DoNotBlockEachOther()
    {
        string table = $"Readers_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");
        Execute($"INSERT INTO {table} (Id, Name) VALUES (1, 'Alice');");
        Execute($"INSERT INTO {table} (Id, Name) VALUES (2, 'Bob');");
        Execute($"INSERT INTO {table} (Id, Name) VALUES (3, 'Charlie');");

        Guid session1 = CreateSession();
        Guid session2 = CreateSession();

        var holderReady = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHolder = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        Task holderTask = Task.Run(() =>
        {
            Engine.LockManager.AcquireReadLock(TestDb, table);
            try
            {
                holderReady.SetResult();
                releaseHolder.Task.GetAwaiter().GetResult();
            }
            finally
            {
                Engine.LockManager.ReleaseReadLock(TestDb, table);
            }
        });

        await holderReady.Task.WaitAsync(TimeSpan.FromSeconds(2));
        try
        {
            Task<QueryResult> selectTask1 = Task.Run(() => ExecuteAndReturnForSession(session1, $"SELECT * FROM {table};"));
            Task<QueryResult> selectTask2 = Task.Run(() => ExecuteAndReturnForSession(session2, $"SELECT * FROM {table};"));

            QueryResult result1 = await selectTask1.WaitAsync(TimeSpan.FromSeconds(2));
            QueryResult result2 = await selectTask2.WaitAsync(TimeSpan.FromSeconds(2));

            Assert.False(result1.IsError);
            Assert.False(result2.IsError);
            Assert.Equal(3, result1.Data.Count);
            Assert.Equal(3, result2.Data.Count);
        }
        finally
        {
            releaseHolder.TrySetResult();
            await holderTask.WaitAsync(TimeSpan.FromSeconds(2));
        }
    }

    [Fact]
    [BrowserTranslateIgnore("Session-lock orchestration test is not representable as a single generated browser SQL scenario")]
    public async Task AutoCommitWrite_WaitsForExistingWriteLock_ThenSucceeds()
    {
        string table = $"Writers_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");

        Guid session = CreateSession();

        var holderReady = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHolder = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        Task holderTask = Task.Run(() =>
        {
            Engine.LockManager.AcquireWriteLock(TestDb, table);
            try
            {
                holderReady.SetResult();
                releaseHolder.Task.GetAwaiter().GetResult();
            }
            finally
            {
                Engine.LockManager.ReleaseWriteLock(TestDb, table);
            }
        });

        await holderReady.Task.WaitAsync(TimeSpan.FromSeconds(2));
        try
        {
            Task insertTask = Task.Run(() => ExecuteForSession(session, $"INSERT INTO {table} (Id, Name) VALUES (1, 'Alice');"));

            await Task.Delay(200);
            Assert.False(insertTask.IsCompleted);

            releaseHolder.TrySetResult();
            await holderTask.WaitAsync(TimeSpan.FromSeconds(2));
            await insertTask.WaitAsync(TimeSpan.FromSeconds(2));
        }
        finally
        {
            releaseHolder.TrySetResult();
            await holderTask.WaitAsync(TimeSpan.FromSeconds(2));
        }

        QueryResult result = ExecuteAndReturn($"SELECT * FROM {table};");
        Assert.False(result.IsError);
        Assert.Single(result.Data);
        Assert.Equal("Alice", result.Data[0]["Name"]?.ToString());
    }

    [Fact]
    [BrowserTranslateIgnore("Multi-session concurrent write orchestration is outside generated browser SQL fixture scope")]
    public async Task ConcurrentAutoCommitWrites_DoNotCorruptTable()
    {
        string table = $"BulkWrites_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");

        const int workerCount = 24;
        Guid[] sessions = Enumerable.Range(0, workerCount)
            .Select(_ => CreateSession())
            .ToArray();

        await Task.WhenAll(sessions.Select((session, i) =>
            Task.Run(() => ExecuteForSession(session, $"INSERT INTO {table} (Id, Name) VALUES ({i + 1}, 'User{i + 1}');"))));

        QueryResult result = ExecuteAndReturn($"SELECT * FROM {table};");
        Assert.False(result.IsError);
        Assert.Equal(workerCount, result.Data.Count);
        Assert.Equal(workerCount, result.Data.Select(row => (int)row["Id"]).Distinct().Count());
    }

    [Fact]
    [BrowserTranslateIgnore("Deadlock orchestration uses direct lock-manager primitives across coordinated tasks")]
    public async Task OpposingTableWriteLocks_DetectDeadlock_WithDiagnosticMessage()
    {
        string firstTable = $"DeadlockA_{Guid.NewGuid():N}";
        string secondTable = $"DeadlockB_{Guid.NewGuid():N}";

        Execute($"CREATE TABLE {firstTable} (Id INT PRIMARY KEY, Name VARCHAR(50));");
        Execute($"CREATE TABLE {secondTable} (Id INT PRIMARY KEY, Name VARCHAR(50));");

        var ready = new CountdownEvent(2);
        var startSecondAcquire = new ManualResetEventSlim(false);
        Exception? firstError = null;
        Exception? secondError = null;

        Task first = Task.Run(() =>
        {
            bool acquiredSecond = false;
            Engine.LockManager.AcquireWriteLock(TestDb, firstTable);
            try
            {
                ready.Signal();
                startSecondAcquire.Wait();
                Engine.LockManager.AcquireWriteLock(TestDb, secondTable);
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
                    Engine.LockManager.ReleaseWriteLock(TestDb, secondTable);
                }

                Engine.LockManager.ReleaseWriteLock(TestDb, firstTable);
            }
        });

        Task second = Task.Run(() =>
        {
            bool acquiredSecond = false;
            Engine.LockManager.AcquireWriteLock(TestDb, secondTable);
            try
            {
                ready.Signal();
                startSecondAcquire.Wait();
                Engine.LockManager.AcquireWriteLock(TestDb, firstTable);
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
                    Engine.LockManager.ReleaseWriteLock(TestDb, firstTable);
                }

                Engine.LockManager.ReleaseWriteLock(TestDb, secondTable);
            }
        });

        Assert.True(ready.Wait(3000), "Both workers should acquire first lock before contention phase.");
        startSecondAcquire.Set();
        await Task.WhenAll(first, second).WaitAsync(TimeSpan.FromSeconds(5));

        DeadlockDetectedException deadlock = Assert.Single(new[] { firstError, secondError }.OfType<DeadlockDetectedException>());
        Assert.Contains("Wait-for cycle", deadlock.Message);
        Assert.Contains($"{TestDb}.{firstTable}", deadlock.Message);
    }

    [Fact]
    [BrowserTranslateIgnore("Deadlock orchestration uses direct lock-manager primitives across coordinated tasks")]
    public async Task OpposingRowWriteLocks_DetectDeadlock_WithDiagnosticMessage()
    {
        string table = $"DeadlockRows_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");

        var ready = new CountdownEvent(2);
        var startSecondAcquire = new ManualResetEventSlim(false);
        Exception? firstError = null;
        Exception? secondError = null;

        Task first = Task.Run(() =>
        {
            bool acquiredSecond = false;
            Engine.LockManager.AcquireRowWriteLock(TestDb, table, 1);
            try
            {
                ready.Signal();
                startSecondAcquire.Wait();
                Engine.LockManager.AcquireRowWriteLock(TestDb, table, 2);
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
                    Engine.LockManager.ReleaseRowWriteLock(TestDb, table, 2);
                }

                Engine.LockManager.ReleaseRowWriteLock(TestDb, table, 1);
            }
        });

        Task second = Task.Run(() =>
        {
            bool acquiredSecond = false;
            Engine.LockManager.AcquireRowWriteLock(TestDb, table, 2);
            try
            {
                ready.Signal();
                startSecondAcquire.Wait();
                Engine.LockManager.AcquireRowWriteLock(TestDb, table, 1);
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
                    Engine.LockManager.ReleaseRowWriteLock(TestDb, table, 1);
                }

                Engine.LockManager.ReleaseRowWriteLock(TestDb, table, 2);
            }
        });

        Assert.True(ready.Wait(3000), "Both workers should acquire first row lock before contention phase.");
        startSecondAcquire.Set();
        await Task.WhenAll(first, second).WaitAsync(TimeSpan.FromSeconds(5));

        DeadlockDetectedException deadlock = Assert.Single(new[] { firstError, secondError }.OfType<DeadlockDetectedException>());
        Assert.Contains("Wait-for cycle", deadlock.Message);
        Assert.Contains("#row:", deadlock.Message);
    }

    private Guid CreateSession()
    {
        Guid session = Guid.NewGuid();
        ExecuteForSession(session, $"USE {TestDb};");
        return session;
    }

    private void ExecuteForSession(Guid session, string sql)
    {
        var engine = new QueryEngine(sql, session, Engine);
        var results = engine.Parse();

        EnsureSuccess(results, sql);
    }

    private QueryResult ExecuteAndReturnForSession(Guid session, string sql)
    {
        var engine = new QueryEngine(sql, session, Engine);
        var results = engine.Parse();

        EnsureSuccess(results, sql);
        return results.Last();
    }

    private static void EnsureSuccess(List<QueryResult> results, string sql)
    {
        foreach (var result in results)
        {
            if (result.IsError || result.Messages.Any(m => !m.Contains("Rows affected")
                                                           && !m.Contains("Rows selected")
                                                           && !m.Contains("Database")
                                                           && !m.Contains("Table")
                                                           && !m.Contains("VACUUM")
                                                           && !m.Contains("Transaction")))
            {
                string errors = string.Join(", ", result.Messages);
                throw new Exception($"SQL Execution Failed for '{sql}':\n{errors}");
            }
        }
    }
}

public class InMemoryConcurrencyTests : ConcurrencyTestsBase
{
    public InMemoryConcurrencyTests() : base(new DataVoConfig { StorageMode = StorageMode.InMemory, LockAcquireTimeoutMs = 1000 }, "ConcurrencyDb_Mem") { }
}

public class DiskConcurrencyTests : ConcurrencyTestsBase
{
    public DiskConcurrencyTests() : base(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = "./test_datavo_concurrency", LockAcquireTimeoutMs = 1000 }, "ConcurrencyDb_Disk") { }
}
