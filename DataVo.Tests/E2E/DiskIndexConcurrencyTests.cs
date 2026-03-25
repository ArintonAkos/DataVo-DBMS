using DataVo.Core.StorageEngine.Config;
using DataVo.Core.Parser;

namespace DataVo.Tests.E2E;

public class DiskIndexConcurrencyTests : SqlExecutionTestsBase
{
    public DiskIndexConcurrencyTests()
        : base(new DataVoConfig
        {
            StorageMode = StorageMode.Disk,
            DiskStoragePath = "./test_datavo_index_concurrency"
        }, "IndexConcurrencyDb_Disk")
    {
    }

    [Fact]
    public async Task ConcurrentInserts_WithSecondaryIndex_PreserveRowsAndIndexMembership()
    {
        string table = $"IndexedUsers_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");
        var createIndexResult = ExecuteAndReturn($"CREATE INDEX idx_name ON {table} (Name);");
        Assert.False(createIndexResult.IsError, string.Join(" | ", createIndexResult.Messages));

        const int workerCount = 32;
        Guid[] sessions = Enumerable.Range(0, workerCount)
            .Select(_ =>
            {
                Guid session = Guid.NewGuid();
                ExecuteForSession(session, $"USE {TestDb};");
                return session;
            })
            .ToArray();

        await Task.WhenAll(sessions.Select((session, i) =>
            Task.Run(() =>
                ExecuteForSession(session, $"INSERT INTO {table} (Id, Name) VALUES ({i + 1}, 'User{i + 1}');"))));

        var result = ExecuteAndReturn($"SELECT Id, Name FROM {table};");
        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(workerCount, result.Data.Count);

        for (int i = 1; i <= workerCount; i++)
        {
            Assert.True(
            Engine.IndexManager.IndexContainsKey($"User{i}", "idx_name", table, TestDb),
            $"Expected key User{i} to be present in idx_name for {table}.");
        }
    }

    [Fact]
    public async Task ConcurrentDeletesAndInserts_WithSecondaryIndex_PreserveCountAndKeyMembership()
    {
        string table = $"IndexedUsersMix_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");
        var createIndexResult = ExecuteAndReturn($"CREATE INDEX idx_name_mix ON {table} (Name);");
        Assert.False(createIndexResult.IsError, string.Join(" | ", createIndexResult.Messages));

        const int seedRows = 100;
        for (int i = 1; i <= seedRows; i++)
        {
            Execute($"INSERT INTO {table} (Id, Name) VALUES ({i}, 'Seed{i}');");
        }

        const int mutationCount = 40;
        Guid[] sessions = Enumerable.Range(0, mutationCount * 2)
            .Select(_ =>
            {
                Guid session = Guid.NewGuid();
                ExecuteForSession(session, $"USE {TestDb};");
                return session;
            })
            .ToArray();

        var tasks = new List<Task>(mutationCount * 2);

        // Delete deterministic seed keys [1..40] while inserting [1001..1040].
        for (int i = 1; i <= mutationCount; i++)
        {
            int idToDelete = i;
            Guid deleteSession = sessions[i - 1];
            tasks.Add(Task.Run(() => ExecuteForSession(deleteSession, $"DELETE FROM {table} WHERE Id = {idToDelete};")));

            int idToInsert = 1000 + i;
            Guid insertSession = sessions[mutationCount + i - 1];
            tasks.Add(Task.Run(() => ExecuteForSession(insertSession, $"INSERT INTO {table} (Id, Name) VALUES ({idToInsert}, 'New{idToInsert}');")));
        }

        await Task.WhenAll(tasks);

        var result = ExecuteAndReturn($"SELECT Id, Name FROM {table};");
        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(seedRows, result.Data.Count);

        for (int i = 1; i <= mutationCount; i++)
        {
            Assert.False(
                Engine.IndexManager.IndexContainsKey($"Seed{i}", "idx_name_mix", table, TestDb),
                $"Expected deleted key Seed{i} to be absent in idx_name_mix for {table}.");

            int insertedId = 1000 + i;
            Assert.True(
                Engine.IndexManager.IndexContainsKey($"New{insertedId}", "idx_name_mix", table, TestDb),
                $"Expected inserted key New{insertedId} to be present in idx_name_mix for {table}.");
        }
    }

    private void ExecuteForSession(Guid session, string sql)
    {
        var engine = new QueryEngine(sql, session, Engine);
        var results = engine.Parse();

        foreach (var result in results)
        {
            Assert.False(result.IsError, string.Join(" | ", result.Messages));
        }
    }
}
