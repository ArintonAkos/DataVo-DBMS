using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DML;

/// <summary>
/// Statements inside one transaction must build on each other exactly as SQL defines: an UPDATE or DELETE matches
/// rows the transaction inserted or changed earlier by their current values, and savepoints restore all of it.
/// </summary>
public sealed class ChainedTransactionDmlTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_chain_{Guid.NewGuid():N}");
    private readonly Guid _tx = Guid.NewGuid();
    private readonly Guid _other = Guid.NewGuid();

    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Disk, StorageMode.Lsm };

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void InsertThenUpdate_ChangesTheBufferedRow(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _tx);
        Ok(db, "INSERT INTO T (Id, V, G) VALUES (3, 30, 7)", _tx);

        Assert.Equal("Rows affected: 1", Ok(db, "UPDATE T SET V = 31 WHERE Id = 3", _tx).Messages.Single());
        Assert.Equal("Rows affected: 1", Ok(db, "UPDATE T SET G = 8 WHERE V = 31", _tx).Messages.Single());

        Assert.Equal((31, 8), Row(db, 3, _tx));
        Assert.Empty(Ok(db, "SELECT Id FROM T WHERE Id = 3", _other).Data);

        Ok(db, "COMMIT", _tx);
        Assert.Equal((31, 8), Row(db, 3, _other));
        Assert.Equal(new object?[] { 3 }, Ids(Ok(db, "SELECT Id FROM T WHERE G = 8", _other)));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void InsertThenDelete_LeavesNothing(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _tx);
        Ok(db, "INSERT INTO T (Id, V, G) VALUES (3, 30, 7)", _tx);
        Ok(db, "INSERT INTO T (Id, V, G) VALUES (4, 40, 7)", _tx);
        Ok(db, "INSERT INTO T (Id, V, G) VALUES (5, 50, 7)", _tx);

        Assert.Equal("Rows affected: 2", Ok(db, "DELETE FROM T WHERE V >= 40", _tx).Messages.Single());
        Assert.Equal(new object?[] { 1, 2, 3 }, Ids(Ok(db, "SELECT Id FROM T", _tx)));

        // The deleted keys are free again inside the transaction.
        Ok(db, "INSERT INTO T (Id, V, G) VALUES (4, 41, 7)", _tx);
        Ok(db, "COMMIT", _tx);

        Assert.Equal(new object?[] { 1, 2, 3, 4 }, Ids(Ok(db, "SELECT Id FROM T", _other)));
        Assert.Equal((41, 7), Row(db, 4, _other));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void UpdateThenUpdate_MatchesTheUpdatedValueAndCoalesces(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _tx);
        Ok(db, "UPDATE T SET V = 99 WHERE Id = 1", _tx);

        Assert.Equal("Rows affected: 1", Ok(db, "UPDATE T SET V = V + 1 WHERE V = 99", _tx).Messages.Single());
        Assert.Equal("Rows affected: 0", Ok(db, "UPDATE T SET V = 0 WHERE V = 10", _tx).Messages.Single());
        Assert.Equal("Rows affected: 1", Ok(db, "UPDATE T SET G = 9 WHERE V = 100", _tx).Messages.Single());

        Core.Transactions.TransactionContext context = db.Engine.TransactionManager.GetContext(_tx)!;
        Assert.Single(context.UpdatedRows["T"]);
        Assert.Equal((100, 9), Row(db, 1, _tx));
        Assert.Equal(new object?[] { 1 }, Ids(Ok(db, "SELECT Id FROM T WHERE G = 9", _tx)));
        Assert.Equal((10, 7), Row(db, 1, _other));

        Ok(db, "COMMIT", _tx);
        Assert.Equal((100, 9), Row(db, 1, _other));
        Assert.Equal(new object?[] { 2 }, Ids(Ok(db, "SELECT Id FROM T WHERE G = 7", _other)));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void UpdateThenDelete_MatchesTheUpdatedValue(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _tx);
        Ok(db, "UPDATE T SET V = 99 WHERE Id = 1", _tx);

        Assert.Equal("Rows affected: 0", Ok(db, "DELETE FROM T WHERE V = 10", _tx).Messages.Single());
        Assert.Equal("Rows affected: 1", Ok(db, "DELETE FROM T WHERE V = 99", _tx).Messages.Single());
        Assert.Equal(new object?[] { 2 }, Ids(Ok(db, "SELECT Id FROM T", _tx)));

        Ok(db, "COMMIT", _tx);
        Assert.Equal(new object?[] { 2 }, Ids(Ok(db, "SELECT Id FROM T", _other)));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void PrimaryKeyChanges_SeeTheTransactionsOwnKeys(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _tx);
        Ok(db, "INSERT INTO T (Id, V, G) VALUES (3, 30, 7)", _tx);

        // Key 3 is taken by the transaction's own insert.
        Assert.True(db.Execute("UPDATE T SET Id = 3 WHERE Id = 2", _tx)[^1].IsError);

        // Key 1 is freed by the transaction's own delete.
        Ok(db, "DELETE FROM T WHERE Id = 1", _tx);
        Assert.Equal("Rows affected: 1", Ok(db, "UPDATE T SET Id = 1 WHERE Id = 2", _tx).Messages.Single());
        Assert.Equal(new object?[] { 1, 3 }, Ids(Ok(db, "SELECT Id FROM T", _tx)));
        Assert.Equal((20, 7), Row(db, 1, _tx));

        Ok(db, "COMMIT", _tx);
        Assert.Equal(new object?[] { 1, 3 }, Ids(Ok(db, "SELECT Id FROM T", _other)));
        Assert.Equal((20, 7), Row(db, 1, _other));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Savepoints_RestoreInsertsUpdatesAndDeletes(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _tx);
        Ok(db, "INSERT INTO T (Id, V, G) VALUES (3, 30, 7)", _tx);
        Ok(db, "UPDATE T SET V = 11 WHERE Id = 1", _tx);
        Ok(db, "SAVEPOINT s1", _tx);

        Ok(db, "UPDATE T SET V = 31 WHERE Id = 3", _tx);
        Ok(db, "UPDATE T SET V = 12 WHERE V = 11", _tx);
        Ok(db, "DELETE FROM T WHERE Id = 2", _tx);
        Ok(db, "INSERT INTO T (Id, V, G) VALUES (4, 40, 7)", _tx);
        Ok(db, "SAVEPOINT s2", _tx);
        Ok(db, "DELETE FROM T WHERE Id = 3", _tx);
        Assert.Equal(new object?[] { 1, 4 }, Ids(Ok(db, "SELECT Id FROM T", _tx)));

        Ok(db, "ROLLBACK TO SAVEPOINT s2", _tx);
        Assert.Equal(new object?[] { 1, 3, 4 }, Ids(Ok(db, "SELECT Id FROM T", _tx)));
        Assert.Equal((31, 7), Row(db, 3, _tx));

        Ok(db, "ROLLBACK TO SAVEPOINT s1", _tx);
        Assert.Equal(new object?[] { 1, 2, 3 }, Ids(Ok(db, "SELECT Id FROM T", _tx)));
        Assert.Equal((30, 7), Row(db, 3, _tx));
        Assert.Equal((11, 7), Row(db, 1, _tx));
        Assert.Equal((20, 7), Row(db, 2, _tx));

        Ok(db, "COMMIT", _tx);
        Assert.Equal(new object?[] { 1, 2, 3 }, Ids(Ok(db, "SELECT Id FROM T", _other)));
        Assert.Equal((11, 7), Row(db, 1, _other));
        Assert.Equal((30, 7), Row(db, 3, _other));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void JoinsAndSubqueries_SeeChainedWrites(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "CREATE TABLE U (Id INT PRIMARY KEY, Tid INT)", _other);
        Ok(db, "BEGIN", _tx);
        Ok(db, "INSERT INTO U (Id, Tid) VALUES (1, 3)", _tx);
        Ok(db, "INSERT INTO T (Id, V, G) VALUES (3, 30, 7)", _tx);
        Ok(db, "UPDATE T SET V = 33 WHERE Id IN (SELECT Tid FROM U)", _tx);

        Assert.Equal(33, Assert.Single(Ok(db, "SELECT t.V FROM U u JOIN T t ON u.Tid = t.Id", _tx).Data).Values.Single());
        Ok(db, "DELETE FROM T WHERE Id IN (SELECT Tid FROM U)", _tx);
        Assert.Empty(Ok(db, "SELECT t.V FROM U u JOIN T t ON u.Tid = t.Id", _tx).Data);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void FailedStatement_LeavesEarlierWritesOfTheTransaction(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _tx);
        Ok(db, "INSERT INTO T (Id, V, G) VALUES (3, 30, 7)", _tx);
        Assert.True(db.Execute("UPDATE T SET Id = 2 WHERE Id = 3", _tx)[^1].IsError);

        Assert.Equal((30, 7), Row(db, 3, _tx));
        Ok(db, "COMMIT", _tx);
        Assert.Equal((30, 7), Row(db, 3, _other));
    }

    /// <summary>
    /// Random statement sequences inside one transaction, checked after every statement against a model of what the
    /// transaction should see, and after COMMIT against what everyone sees.
    /// </summary>
    [Theory]
    [MemberData(nameof(Modes))]
    public void RandomChains_MatchAModel(StorageMode mode)
    {
        for (int seed = 1; seed <= 6; seed++)
        {
            RunRandomChain(mode, seed);
        }
    }

    private void RunRandomChain(StorageMode mode, int seed)
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }

        using DataVoContext db = Seed(mode);
        var random = new Random(seed);
        var committed = new SortedDictionary<int, (int V, int G)> { [1] = (10, 7), [2] = (20, 7) };
        var model = new SortedDictionary<int, (int V, int G)>(committed);
        var savepoints = new List<(string Name, SortedDictionary<int, (int V, int G)> State)>();

        Ok(db, "BEGIN", _tx);
        for (int step = 0; step < 40; step++)
        {
            int key = random.Next(1, 9);
            int value = random.Next(0, 5) * 10;
            switch (random.Next(7))
            {
                case 0:
                case 1:
                    if (model.ContainsKey(key))
                    {
                        Assert.True(db.Execute($"INSERT INTO T (Id, V, G) VALUES ({key}, {value}, 7)", _tx)[^1].IsError);
                    }
                    else
                    {
                        Ok(db, $"INSERT INTO T (Id, V, G) VALUES ({key}, {value}, 7)", _tx);
                        model[key] = (value, 7);
                    }

                    break;
                case 2:
                    {
                        int g = random.Next(7, 10);
                        int expected = model.Count(row => row.Value.V == value);
                        Assert.Equal($"Rows affected: {expected}", Ok(db, $"UPDATE T SET G = {g}, V = V + 1 WHERE V = {value}", _tx).Messages.Single());
                        foreach (int id in model.Where(row => row.Value.V == value).Select(row => row.Key).ToList())
                        {
                            model[id] = (model[id].V + 1, g);
                        }

                        break;
                    }

                case 3:
                    {
                        int expected = model.ContainsKey(key) ? 1 : 0;
                        Assert.Equal($"Rows affected: {expected}", Ok(db, $"UPDATE T SET V = {value} WHERE Id = {key}", _tx).Messages.Single());
                        if (expected == 1)
                        {
                            model[key] = (value, model[key].G);
                        }

                        break;
                    }

                case 4:
                    {
                        int g = random.Next(7, 10);
                        int expected = model.Count(row => row.Value.G == g);
                        Assert.Equal($"Rows affected: {expected}", Ok(db, $"DELETE FROM T WHERE G = {g}", _tx).Messages.Single());
                        foreach (int id in model.Where(row => row.Value.G == g).Select(row => row.Key).ToList())
                        {
                            model.Remove(id);
                        }

                        break;
                    }

                case 5:
                    {
                        string name = $"s{random.Next(3)}";
                        Ok(db, $"SAVEPOINT {name}", _tx);
                        savepoints.RemoveAll(savepoint => savepoint.Name == name);
                        savepoints.Add((name, new SortedDictionary<int, (int V, int G)>(model)));
                        break;
                    }

                default:
                    if (savepoints.Count > 0)
                    {
                        int index = random.Next(savepoints.Count);
                        Ok(db, $"ROLLBACK TO SAVEPOINT {savepoints[index].Name}", _tx);
                        model = new SortedDictionary<int, (int V, int G)>(savepoints[index].State);
                        savepoints.RemoveRange(index + 1, savepoints.Count - index - 1);
                    }

                    break;
            }

            AssertMatches(db, model, _tx, $"seed {seed} step {step}");
            AssertMatches(db, committed, _other, $"seed {seed} step {step} (other session)");
        }

        Ok(db, "COMMIT", _tx);
        AssertMatches(db, model, _other, $"seed {seed} after commit");
    }

    private static void AssertMatches(DataVoContext db, SortedDictionary<int, (int V, int G)> model, Guid session, string because)
    {
        List<Dictionary<string, object?>> rows = Ok(db, "SELECT Id, V, G FROM T", session).Data;
        var actual = rows
            .Select(row => ((int)row["Id"]!, ((int)row["V"]!, (int)row["G"]!)))
            .OrderBy(row => row.Item1)
            .ToList();
        Assert.True(
            model.Select(pair => (pair.Key, pair.Value)).SequenceEqual(actual),
            $"{because}: expected [{string.Join(", ", model)}] but read [{string.Join(", ", actual)}]");

        foreach (int g in new[] { 7, 8, 9 })
        {
            object?[] expectedIds = [.. model.Where(pair => pair.Value.G == g).Select(pair => (object?)pair.Key)];
            Assert.True(expectedIds.SequenceEqual(Ids(Ok(db, $"SELECT Id FROM T WHERE G = {g}", session))), $"{because}: index lookup G = {g}");
        }
    }

    private (int V, int G) Row(DataVoContext db, int id, Guid session)
    {
        Dictionary<string, object?> row = Assert.Single(Ok(db, $"SELECT V, G FROM T WHERE Id = {id}", session).Data);
        return ((int)row["V"]!, (int)row["G"]!);
    }

    private static object?[] Ids(QueryResult result) =>
        [.. result.Data.Select(row => row["Id"]).OrderBy(id => (int)id!)];

    private DataVoContext Seed(StorageMode mode)
    {
        var db = new DataVoContext(new DataVoConfig
        {
            StorageMode = mode,
            DiskStoragePath = mode == StorageMode.InMemory ? null! : _directory,
            LsmStrictFsync = false,
        });
        Ok(db, "CREATE DATABASE Chain", _tx);
        Ok(db, "USE Chain", _tx);
        Ok(db, "USE Chain", _other);
        Ok(db, "CREATE TABLE T (Id INT PRIMARY KEY, V INT, G INT)", _tx);
        Ok(db, "CREATE INDEX ix_g ON T (G)", _tx);
        Ok(db, "INSERT INTO T (Id, V, G) VALUES (1, 10, 7)", _tx);
        Ok(db, "INSERT INTO T (Id, V, G) VALUES (2, 20, 7)", _tx);
        return db;
    }

    private static QueryResult Ok(DataVoContext db, string sql, Guid session)
    {
        QueryResult result = db.Execute(sql, session)[^1];
        Assert.False(result.IsError, $"{sql} => {string.Join(" | ", result.Messages)}");
        return result;
    }
}
