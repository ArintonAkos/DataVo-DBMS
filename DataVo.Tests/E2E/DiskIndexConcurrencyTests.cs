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
