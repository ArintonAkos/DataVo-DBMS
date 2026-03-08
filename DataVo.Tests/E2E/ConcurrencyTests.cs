using DataVo.Core.Contracts.Results;
using DataVo.Core.Parser;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.Transactions;

namespace DataVo.Tests.E2E;

public abstract class ConcurrencyTestsBase(DataVoConfig config, string testDbName) : SqlExecutionTestsBase(config, testDbName)
{
    [Fact]
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
            LockManager.Instance.AcquireReadLock(TestDb, table);
            try
            {
                holderReady.SetResult();
                releaseHolder.Task.GetAwaiter().GetResult();
            }
            finally
            {
                LockManager.Instance.ReleaseReadLock(TestDb, table);
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
    public async Task AutoCommitWrite_WaitsForExistingWriteLock_ThenSucceeds()
    {
        string table = $"Writers_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");

        Guid session = CreateSession();

        var holderReady = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHolder = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        Task holderTask = Task.Run(() =>
        {
            LockManager.Instance.AcquireWriteLock(TestDb, table);
            try
            {
                holderReady.SetResult();
                releaseHolder.Task.GetAwaiter().GetResult();
            }
            finally
            {
                LockManager.Instance.ReleaseWriteLock(TestDb, table);
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

    private Guid CreateSession()
    {
        Guid session = Guid.NewGuid();
        ExecuteForSession(session, $"USE {TestDb};");
        return session;
    }

    private void ExecuteForSession(Guid session, string sql)
    {
        var engine = new QueryEngine(sql, session);
        var results = engine.Parse();

        EnsureSuccess(results, sql);
    }

    private QueryResult ExecuteAndReturnForSession(Guid session, string sql)
    {
        var engine = new QueryEngine(sql, session);
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

[Collection("SequentialStorageTests")]
public class InMemoryConcurrencyTests : ConcurrencyTestsBase
{
    public InMemoryConcurrencyTests() : base(new DataVoConfig { StorageMode = StorageMode.InMemory }, "ConcurrencyDb_Mem") { }
}

[Collection("SequentialStorageTests")]
public class DiskConcurrencyTests : ConcurrencyTestsBase
{
    public DiskConcurrencyTests() : base(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = "./test_datavo_concurrency" }, "ConcurrencyDb_Disk") { }
}
