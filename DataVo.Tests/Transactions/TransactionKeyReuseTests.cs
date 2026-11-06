using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Transactions;

/// <summary>
/// Inside a transaction, key checks see the transaction's own buffered writes: a key it deletes (or moves
/// away with an UPDATE) can be inserted again, and a key it already inserted (or moved to) cannot. COMMIT
/// applies deletes and updates before inserts, so storage ends up as the statements described.
/// </summary>
public sealed class TransactionKeyReuseTests : IDisposable
{
    private readonly List<string> _directories = [];

    public void Dispose()
    {
        foreach (string directory in _directories)
        {
            try { Directory.Delete(directory, recursive: true); } catch (IOException) { }
        }
    }

    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    [Theory]
    [MemberData(nameof(Modes))]
    public void DeleteThenInsert_TheSameKey_Commits(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Run(db, "BEGIN TRANSACTION");
        Run(db, "DELETE FROM T WHERE Id = 1");
        Run(db, "INSERT INTO T (Id, E, V) VALUES (1, 'a2', 'again')");
        Run(db, "COMMIT");

        Assert.Equal(["1:again", "2:b"], Rows(db));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void InsertingTheSameKeyTwice_InOneTransaction_IsRejected(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Run(db, "BEGIN TRANSACTION");
        Run(db, "INSERT INTO T (Id, E, V) VALUES (5, 'e5', 'x')");
        Assert.True(db.Execute("INSERT INTO T (Id, E, V) VALUES (5, 'e6', 'y')").Last().IsError);
        Run(db, "COMMIT");

        Assert.Equal(["1:a", "2:b", "5:x"], Rows(db));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void MovingAKeyAway_ThenInsertingIt_Commits(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Run(db, "BEGIN TRANSACTION");
        Run(db, "UPDATE T SET Id = 10 WHERE Id = 1");
        Run(db, "INSERT INTO T (Id, E, V) VALUES (1, 'new', 'fresh')");
        Run(db, "COMMIT");

        Assert.Equal(["1:fresh", "2:b", "10:a"], Rows(db));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void InsertingAKey_AnUpdateMovedInto_IsRejected(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Run(db, "BEGIN TRANSACTION");
        Run(db, "UPDATE T SET Id = 7 WHERE Id = 2");
        Assert.True(db.Execute("INSERT INTO T (Id, E, V) VALUES (7, 'z', 'clash')").Last().IsError);
        Run(db, "COMMIT");

        Assert.Equal(["1:a", "7:b"], Rows(db));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void AUniqueValue_FreedByADelete_CanBeReused(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Run(db, "BEGIN TRANSACTION");
        Run(db, "DELETE FROM T WHERE Id = 2");
        Run(db, "INSERT INTO T (Id, E, V) VALUES (3, 'b@x', 'reused')");
        Run(db, "COMMIT");

        Assert.Equal(["1:a", "3:reused"], Rows(db));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void UpdateThenDelete_ThenInsert_DoesNotApplyTheUpdateToTheNewRow(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Run(db, "BEGIN TRANSACTION");
        Run(db, "UPDATE T SET V = 'changed' WHERE Id = 1");
        Run(db, "DELETE FROM T WHERE Id = 1");
        Run(db, "INSERT INTO T (Id, E, V) VALUES (1, 'n', 'inserted')");
        Run(db, "COMMIT");

        Assert.Equal(["1:inserted", "2:b"], Rows(db));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Rollback_LeavesTheOriginalRows(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Run(db, "BEGIN TRANSACTION");
        Run(db, "DELETE FROM T WHERE Id = 1");
        Run(db, "INSERT INTO T (Id, E, V) VALUES (1, 'a2', 'again')");
        Run(db, "ROLLBACK");

        Assert.Equal(["1:a", "2:b"], Rows(db));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void OutsideATransaction_ADuplicateKey_IsStillRejected(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Assert.True(db.Execute("INSERT INTO T (Id, E, V) VALUES (1, 'q', 'dup')").Last().IsError);
        Assert.True(db.Execute("INSERT INTO T (Id, E, V) VALUES (9, 'a@x', 'dup')").Last().IsError);
    }

    private DataVoContext Open(StorageMode mode)
    {
        string directory = Path.Combine(Path.GetTempPath(), "datavo-tx-keys-" + Guid.NewGuid().ToString("N"));
        _directories.Add(directory);
        var db = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = directory, LsmStrictFsync = false });
        Run(db, "CREATE DATABASE D");
        Run(db, "USE D");
        Run(db, "CREATE TABLE T (Id INT PRIMARY KEY, E VARCHAR(10) UNIQUE, V VARCHAR(10))");
        Run(db, "INSERT INTO T (Id, E, V) VALUES (1, 'a@x', 'a'), (2, 'b@x', 'b')");
        return db;
    }

    private static string[] Rows(DataVoContext db)
    {
        var result = db.Execute("SELECT Id, V FROM T ORDER BY Id").Last();
        Assert.False(result.IsError, string.Join("; ", result.Messages));
        return result.Data.Select(row => $"{row["Id"]}:{row["V"]}").ToArray();
    }

    private static void Run(DataVoContext db, string sql)
    {
        var result = db.Execute(sql).Last();
        Assert.False(result.IsError, $"{sql}: {string.Join("; ", result.Messages)}");
    }
}
