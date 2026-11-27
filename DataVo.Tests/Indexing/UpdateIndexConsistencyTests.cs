using DataVo.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Indexing;

/// <summary>
/// Updating a row through one write path and deleting it through another must not leave its primary key
/// behind in any index lane. A stale scalar entry used to make the next insert of the key fail with a primary
/// key violation for a row that no longer existed.
/// </summary>
public sealed class UpdateIndexConsistencyTests : IDisposable
{
    private readonly List<DataVoContext> _contexts = [];
    private readonly List<string> _roots = [];

    public static TheoryData<StorageMode> Modes => [StorageMode.InMemory, StorageMode.Lsm];

    public void Dispose()
    {
        _contexts.ForEach(context => context.Dispose());
        foreach (string root in _roots)
        {
            try
            {
                Directory.Delete(root, recursive: true);
            }
            catch (IOException)
            {
            }
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void SqlUpdate_ThenCompiledUpdate_ThenDelete_LeavesTheKeyFree(StorageMode mode)
    {
        DataVoContext context = Open(mode);
        Ok(context, "INSERT INTO P VALUES (5, 1, 'a')");
        Ok(context, "UPDATE P SET Hp = 99 WHERE Id = 5");
        var plan = DataVoCompiledQueryPlan.Update("P", new Dictionary<string, string> { ["Hp"] = "hp" }, whereColumn: "Id", whereParameterName: "id");
        Assert.Equal(1, DataVoCompiledQuery.Update(context, plan, [new DataVoCompiledQueryParameter("hp", 7), new DataVoCompiledQueryParameter("id", 5)]));
        Ok(context, "DELETE FROM P WHERE Id = 5");

        context.BulkInsert("P", [new Dictionary<string, object?> { ["Id"] = 5, ["Hp"] = 2, ["Name"] = "b" }]);

        Assert.Equal("5:2", Single(context, "SELECT Id, Hp FROM P WHERE Id = 5"));
    }

    [Fact]
    public void ScalarCleanup_LeavesTheIntegerFastLaneEntryInPlace()
    {
        DataVoContext context = Open(StorageMode.InMemory);
        Ok(context, "INSERT INTO P VALUES (1, 1, 'a')");
        var indexes = context.Engine.IndexManager;
        Assert.True(indexes.HasIntegerPrimaryKeyFastLane("_PK_P", "P", "U"));
        Assert.True(indexes.TryLookupIntegerPrimaryKey(1, "_PK_P", "P", "U", out RowKey rowId));
        string key = DataVo.Core.BTree.IndexKeyEncoder.BuildKeyString(new Dictionary<string, object?> { ["Id"] = 1 }, ["Id"]);

        indexes.DeleteScalarIndexEntries([(key, rowId)], "_PK_P", "P", "U");

        Assert.True(indexes.TryLookupIntegerPrimaryKey(1, "_PK_P", "P", "U", out RowKey still));
        Assert.Equal(rowId, still);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task UpdatesThatKeepTheKey_NeverHideTheRowFromKeyedReaders(StorageMode mode)
    {
        DataVoContext context = Open(mode);
        Ok(context, "INSERT INTO P VALUES (1, 0, 'a')");
        var hp = new DataVoBatchLayout("P", ["Id", "Hp"]);
        int misses = 0;
        using var stop = new CancellationTokenSource();
        Task reader = Task.Run(() =>
        {
            while (!stop.IsCancellationRequested)
            {
                if (!context.Engine.IndexManager.FilterUsingIndex("1", "_PK_P", "P", "U").Any())
                {
                    Interlocked.Increment(ref misses);
                }
            }
        });

        var batch = new DataVoWriteBatch();
        for (int i = 1; i <= 2_000; i++)
        {
            batch.Clear();
            batch.Update(hp, 1, i);
            context.ApplyBatch(batch);
        }

        stop.Cancel();
        await reader;
        Assert.Equal(0, misses);
        Assert.Equal("1:2000", Single(context, "SELECT Id, Hp FROM P WHERE Id = 1"));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void RandomMixOfSqlAndBatches_KeepsKeyedLookupsAndScansInAgreement(StorageMode mode)
    {
        var full = new DataVoBatchLayout("P", ["Id", "Hp", "Name"]);
        var hp = new DataVoBatchLayout("P", ["Id", "Hp"]);
        var key = new DataVoBatchLayout("P", ["Id"]);
        var random = new Random(12);
        DataVoContext context = Open(mode);
        for (int step = 0; step < 400; step++)
        {
            int id = random.Next(1, 8);
            if (random.Next(2) == 0)
            {
                bool exists = context.Execute($"SELECT Id FROM P WHERE Id = {id}")[^1].Data.Count > 0;
                Ok(context, !exists ? $"INSERT INTO P VALUES ({id}, {random.Next(100)}, 'n')"
                    : random.Next(3) == 0 ? $"DELETE FROM P WHERE Id = {id}"
                    : $"UPDATE P SET Hp = {random.Next(100)} WHERE Id = {id}");
            }
            else
            {
                var batch = new DataVoWriteBatch();
                for (int i = random.Next(1, 4); i > 0; i--)
                {
                    int target = random.Next(1, 8);
                    switch (random.Next(3))
                    {
                        case 0: batch.Delete(key, target); break;
                        case 1: batch.Upsert(full, target, random.Next(100), "r"); break;
                        default: batch.Update(hp, target, random.Next(100)); break;
                    }
                }

                context.ApplyBatch(batch);
            }

            HashSet<int> scanned = [.. context.Execute("SELECT Id FROM P")[^1].Data.Select(row => (int)row["Id"]!)];
            for (int k = 1; k < 8; k++)
            {
                Assert.Equal(scanned.Contains(k), context.Execute($"SELECT Id FROM P WHERE Id = {k}")[^1].Data.Count > 0);
            }
        }

        // Every key can be deleted and inserted again through the dictionary insert path.
        Ok(context, "DELETE FROM P WHERE Id > 0");
        context.BulkInsert("P", [.. Enumerable.Range(1, 7).Select(id => (IReadOnlyDictionary<string, object?>)new Dictionary<string, object?> { ["Id"] = id, ["Hp"] = 0, ["Name"] = "x" })]);
        Assert.Equal(7, context.Execute("SELECT Id FROM P")[^1].Data.Count);
    }

    private DataVoContext Open(StorageMode mode)
    {
        string? root = mode == StorageMode.Lsm ? TestStorageDirectories.Create("update_index") : null;
        if (root is not null)
        {
            _roots.Add(root);
        }

        var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DataDirectory = root ?? string.Empty, LsmStrictFsync = false });
        _contexts.Add(context);
        Ok(context, "CREATE DATABASE U");
        Ok(context, "USE U");
        Ok(context, "CREATE TABLE P (Id INT PRIMARY KEY, Hp INT, Name VARCHAR(20))");
        return context;
    }

    private static string Single(DataVoContext context, string sql)
    {
        var row = Assert.Single(context.Execute(sql)[^1].Data);
        return string.Join(":", row.Values);
    }

    private static void Ok(DataVoContext context, string sql)
    {
        var result = context.Execute(sql)[^1];
        Assert.False(result.IsError, $"{sql}: {string.Join(" | ", result.Messages)}");
    }
}
