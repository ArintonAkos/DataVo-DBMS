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

    [Fact]
    public async Task SeededFuzzLite_ConcurrentIndexMutations_PreserveDeterministicOutcome()
    {
        string table = $"IndexedUsersFuzz_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");
        var createIndexResult = ExecuteAndReturn($"CREATE INDEX idx_name_fuzz ON {table} (Name);");
        Assert.False(createIndexResult.IsError, string.Join(" | ", createIndexResult.Messages));

        const int seedRows = 200;
        for (int i = 1; i <= seedRows; i++)
        {
            Execute($"INSERT INTO {table} (Id, Name) VALUES ({i}, 'Seed{i}');");
        }

        // Fuzz-lite: deterministic random mix keeps the test reproducible.
        var rng = new Random(20260325);
        var deletePool = Enumerable.Range(1, seedRows).OrderBy(_ => rng.Next()).Take(80).ToList();
        var deletedIds = new HashSet<int>();
        var insertedIds = new HashSet<int>();
        var operations = new List<(bool IsDelete, int Id)>(160);
        int nextInsertId = 10_000;

        while (operations.Count < 160)
        {
            bool canDelete = deletePool.Count > 0;
            bool doDelete = canDelete && rng.NextDouble() < 0.5d;

            if (doDelete)
            {
                int pick = deletePool[^1];
                deletePool.RemoveAt(deletePool.Count - 1);
                deletedIds.Add(pick);
                operations.Add((true, pick));
                continue;
            }

            int insertId = nextInsertId++;
            insertedIds.Add(insertId);
            operations.Add((false, insertId));
        }

        Guid[] sessions = Enumerable.Range(0, operations.Count)
            .Select(_ =>
            {
                Guid session = Guid.NewGuid();
                ExecuteForSession(session, $"USE {TestDb};");
                return session;
            })
            .ToArray();

        await Task.WhenAll(operations.Select((op, i) => Task.Run(() =>
        {
            string sql = op.IsDelete
                ? $"DELETE FROM {table} WHERE Id = {op.Id};"
                : $"INSERT INTO {table} (Id, Name) VALUES ({op.Id}, 'Fuzz{op.Id}');";

            ExecuteForSession(sessions[i], sql);
        })));

        int expectedCount = seedRows - deletedIds.Count + insertedIds.Count;

        var result = ExecuteAndReturn($"SELECT Id, Name FROM {table};");
        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(expectedCount, result.Data.Count);

        foreach (int deletedId in deletedIds)
        {
            Assert.False(
                Engine.IndexManager.IndexContainsKey($"Seed{deletedId}", "idx_name_fuzz", table, TestDb),
                $"Expected deleted key Seed{deletedId} to be absent in idx_name_fuzz for {table}.");
        }

        foreach (int insertedId in insertedIds)
        {
            Assert.True(
                Engine.IndexManager.IndexContainsKey($"Fuzz{insertedId}", "idx_name_fuzz", table, TestDb),
                $"Expected inserted key Fuzz{insertedId} to be present in idx_name_fuzz for {table}.");
        }
    }

    [Fact]
    public async Task SeededFuzzLite_WithBoundedUpdates_PreserveStableIndexMembership()
    {
        string table = $"IndexedUsersFuzzUpd_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");
        var createIndexResult = ExecuteAndReturn($"CREATE INDEX idx_name_fuzz_upd ON {table} (Name);");
        Assert.False(createIndexResult.IsError, string.Join(" | ", createIndexResult.Messages));

        const int seedRows = 180;
        for (int i = 1; i <= seedRows; i++)
        {
            Execute($"INSERT INTO {table} (Id, Name) VALUES ({i}, 'Seed{i}');");
        }

        var rng = new Random(20260326);
        var deletePool = Enumerable.Range(1, seedRows).OrderBy(_ => rng.Next()).Take(40).ToList();
        var updatePool = Enumerable.Range(1, seedRows).Where(id => !deletePool.Contains(id)).OrderBy(_ => rng.Next()).Take(40).ToList();

        var deletedIds = new HashSet<int>();
        var insertedIds = new HashSet<int>();
        var updatedIds = new HashSet<int>();
        var operations = new List<(string Kind, int Id)>(120);
        int nextInsertId = 20_000;

        for (int i = 0; i < 40; i++)
        {
            int deleteId = deletePool[i];
            deletedIds.Add(deleteId);
            operations.Add(("delete", deleteId));

            int insertId = nextInsertId++;
            insertedIds.Add(insertId);
            operations.Add(("insert", insertId));

            int updateId = updatePool[i];
            updatedIds.Add(updateId);
            operations.Add(("update", updateId));
        }

        operations = [.. operations.OrderBy(_ => rng.Next())];

        Guid[] sessions = Enumerable.Range(0, operations.Count)
            .Select(_ =>
            {
                Guid session = Guid.NewGuid();
                ExecuteForSession(session, $"USE {TestDb};");
                return session;
            })
            .ToArray();

        await Task.WhenAll(operations.Select((op, i) => Task.Run(() =>
        {
            string sql = op.Kind switch
            {
                "delete" => $"DELETE FROM {table} WHERE Id = {op.Id};",
                "insert" => $"INSERT INTO {table} (Id, Name) VALUES ({op.Id}, 'Fuzz{op.Id}');",
                "update" => $"UPDATE {table} SET Name = 'Upd{op.Id}' WHERE Id = {op.Id};",
                _ => throw new InvalidOperationException($"Unknown operation kind: {op.Kind}")
            };

            ExecuteForSession(sessions[i], sql);
        })));

        int expectedCount = seedRows - deletedIds.Count + insertedIds.Count;
        var result = ExecuteAndReturn($"SELECT Id, Name FROM {table};");
        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(expectedCount, result.Data.Count);

        foreach (int deletedId in deletedIds)
        {
            Assert.False(
                Engine.IndexManager.IndexContainsKey($"Seed{deletedId}", "idx_name_fuzz_upd", table, TestDb),
                $"Expected deleted key Seed{deletedId} to be absent in idx_name_fuzz_upd for {table}.");
            Assert.False(
                Engine.IndexManager.IndexContainsKey($"Upd{deletedId}", "idx_name_fuzz_upd", table, TestDb),
                $"Expected updated key Upd{deletedId} to be absent for deleted row in {table}.");
        }

        foreach (int insertedId in insertedIds)
        {
            Assert.True(
                Engine.IndexManager.IndexContainsKey($"Fuzz{insertedId}", "idx_name_fuzz_upd", table, TestDb),
                $"Expected inserted key Fuzz{insertedId} to be present in idx_name_fuzz_upd for {table}.");
        }

        foreach (int updatedId in updatedIds)
        {
            Assert.False(
                Engine.IndexManager.IndexContainsKey($"Seed{updatedId}", "idx_name_fuzz_upd", table, TestDb),
                $"Expected old key Seed{updatedId} to be absent after update in {table}.");
            Assert.True(
                Engine.IndexManager.IndexContainsKey($"Upd{updatedId}", "idx_name_fuzz_upd", table, TestDb),
                $"Expected updated key Upd{updatedId} to be present in idx_name_fuzz_upd for {table}.");
        }
    }

    [Fact]
    public async Task ConcurrentDuplicatePrimaryKeyInserts_RejectDuplicatesAndKeepSingleRow()
    {
        string table = $"IndexedUsersDup_{Guid.NewGuid():N}";
        Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");
        var createIndexResult = ExecuteAndReturn($"CREATE INDEX idx_name_dup ON {table} (Name);");
        Assert.False(createIndexResult.IsError, string.Join(" | ", createIndexResult.Messages));

        const int contenderCount = 16;
        const int duplicateId = 4242;

        Guid[] sessions = Enumerable.Range(0, contenderCount)
            .Select(_ =>
            {
                Guid session = Guid.NewGuid();
                ExecuteForSession(session, $"USE {TestDb};");
                return session;
            })
            .ToArray();

        var startGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tasks = sessions.Select((session, i) => Task.Run(async () =>
        {
            await startGate.Task;
            string name = $"Dup{i + 1}";
            var results = ExecuteForSessionRaw(session, $"INSERT INTO {table} (Id, Name) VALUES ({duplicateId}, '{name}');");
            return (name, Last: results.Last());
        })).ToArray();

        startGate.SetResult();
        var outcomes = await Task.WhenAll(tasks);

        int successCount = outcomes.Count(o =>
            !o.Last.IsError && !(o.Last.Messages?.Any(m => m.Contains("Primary key violation", StringComparison.OrdinalIgnoreCase)) ?? false));
        int duplicateRejectCount = outcomes.Count(o =>
            (o.Last.Messages?.Any(m => m.Contains("Primary key violation", StringComparison.OrdinalIgnoreCase)) ?? false)
            || o.Last.IsError);

        Assert.Equal(1, successCount);
        Assert.Equal(contenderCount - 1, duplicateRejectCount);

        var rows = ExecuteAndReturn($"SELECT Id, Name FROM {table} WHERE Id = {duplicateId};");
        Assert.False(rows.IsError, string.Join(" | ", rows.Messages));
        Assert.Single(rows.Data);

        string persistedName = rows.Data[0]["Name"]?.ToString() ?? string.Empty;
        Assert.Contains(outcomes.Select(o => o.name), candidate => candidate == persistedName);
        Assert.True(
            Engine.IndexManager.IndexContainsKey(persistedName, "idx_name_dup", table, TestDb),
            $"Expected persisted key {persistedName} to be present in idx_name_dup for {table}.");
    }

    [Fact]
    public async Task ConcurrentDuplicatePrimaryKeyInserts_RepeatedRounds_AlwaysSingleWinner()
    {
        const int rounds = 3;
        const int contenderCount = 12;
        const int duplicateId = 7_777;

        for (int round = 1; round <= rounds; round++)
        {
            string table = $"IndexedUsersDupRound{round}_{Guid.NewGuid():N}";
            Execute($"CREATE TABLE {table} (Id INT PRIMARY KEY, Name VARCHAR(50));");
            var createIndexResult = ExecuteAndReturn($"CREATE INDEX idx_name_dup_round ON {table} (Name);");
            Assert.False(createIndexResult.IsError, string.Join(" | ", createIndexResult.Messages));

            Guid[] sessions = Enumerable.Range(0, contenderCount)
                .Select(_ =>
                {
                    Guid session = Guid.NewGuid();
                    ExecuteForSession(session, $"USE {TestDb};");
                    return session;
                })
                .ToArray();

            var startGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var tasks = sessions.Select((session, i) => Task.Run(async () =>
            {
                await startGate.Task;
                string name = $"Round{round}_Dup{i + 1}";
                var results = ExecuteForSessionRaw(session, $"INSERT INTO {table} (Id, Name) VALUES ({duplicateId}, '{name}');");
                return (name, Last: results.Last());
            })).ToArray();

            startGate.SetResult();
            var outcomes = await Task.WhenAll(tasks);

            int successCount = outcomes.Count(o =>
                !o.Last.IsError && !(o.Last.Messages?.Any(m => m.Contains("Primary key violation", StringComparison.OrdinalIgnoreCase)) ?? false));
            int duplicateRejectCount = outcomes.Count(o =>
                (o.Last.Messages?.Any(m => m.Contains("Primary key violation", StringComparison.OrdinalIgnoreCase)) ?? false)
                || o.Last.IsError);

            Assert.Equal(1, successCount);
            Assert.Equal(contenderCount - 1, duplicateRejectCount);

            var rows = ExecuteAndReturn($"SELECT Id, Name FROM {table} WHERE Id = {duplicateId};");
            Assert.False(rows.IsError, string.Join(" | ", rows.Messages));
            Assert.Single(rows.Data);
        }
    }

    private void ExecuteForSession(Guid session, string sql)
    {
        var results = ExecuteForSessionRaw(session, sql);

        foreach (var result in results)
        {
            Assert.False(result.IsError, string.Join(" | ", result.Messages));
        }
    }

    private List<DataVo.Core.Contracts.Results.QueryResult> ExecuteForSessionRaw(Guid session, string sql)
    {
        var engine = new QueryEngine(sql, session, Engine);
        return engine.Parse();
    }
}
