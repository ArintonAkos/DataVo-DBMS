using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DML;

/// <summary>
/// A snapshot is ordered by commit, not by transaction id: a transaction must not see a transaction that was still
/// open when it began, even when that one began earlier (has a lower id) and commits while it is still open.
/// </summary>
public sealed class CommitOrderSnapshotTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_cos_{Guid.NewGuid():N}");
    private readonly Guid _a = Guid.NewGuid();
    private readonly Guid _b = Guid.NewGuid();
    private readonly Guid _c = Guid.NewGuid();

    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void EarlierTransaction_CommittingLater_StaysInvisible(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _a);
        Ok(db, "BEGIN", _b);
        Ok(db, "INSERT INTO T (Id, V, G) VALUES (3, 30, 7)", _a);
        Ok(db, "UPDATE T SET V = 11 WHERE Id = 1", _a);
        Ok(db, "DELETE FROM T WHERE Id = 2", _a);
        Ok(db, "COMMIT", _a);

        Assert.Equal(new object?[] { 1, 2 }, Ids(Ok(db, "SELECT Id FROM T", _b)));
        Assert.Equal(10, Assert.Single(Ok(db, "SELECT V FROM T WHERE Id = 1", _b).Data)["V"]);
        Assert.Equal(20, Assert.Single(Ok(db, "SELECT V FROM T WHERE G = 7 AND Id = 2", _b).Data)["V"]);
        Assert.Equal(2L, Assert.Single(Ok(db, "SELECT COUNT(*) AS C FROM T", _b).Data)["C"]);

        Ok(db, "COMMIT", _b);
        Assert.Equal(new object?[] { 1, 3 }, Ids(Ok(db, "SELECT Id FROM T", _b)));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void TransactionBegunAfterACommit_SeesIt(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _a);
        Ok(db, "BEGIN", _b);
        Ok(db, "UPDATE T SET V = 11 WHERE Id = 1", _a);
        Ok(db, "COMMIT", _a);
        Ok(db, "BEGIN", _c);

        Assert.Equal(11, Assert.Single(Ok(db, "SELECT V FROM T WHERE Id = 1", _c).Data)["V"]);
        Assert.Equal(10, Assert.Single(Ok(db, "SELECT V FROM T WHERE Id = 1", _b).Data)["V"]);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ConcurrentWritersOfOneRow_FirstCommitterWins(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _a);
        Ok(db, "BEGIN", _b);
        Ok(db, "UPDATE T SET V = 11 WHERE Id = 1", _a);
        Ok(db, "UPDATE T SET V = 12 WHERE Id = 1", _b);
        Ok(db, "INSERT INTO T (Id, V, G) VALUES (9, 90, 7)", _b);
        Ok(db, "COMMIT", _a);

        QueryResult commit = db.Execute("COMMIT", _b)[^1];

        Assert.True(commit.IsError);
        Assert.Equal(11, Assert.Single(Ok(db, "SELECT V FROM T WHERE Id = 1", _c).Data)["V"]);
        Assert.Empty(Ok(db, "SELECT Id FROM T WHERE Id = 9", _c).Data);
        Assert.False(db.Engine.TransactionManager.HasActiveTransaction(_b));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void DeleteByAnEarlierTransaction_CommittedLater_KeepsTheRowForLaterSnapshots(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _a);
        Ok(db, "BEGIN", _b);
        Ok(db, "DELETE FROM T WHERE Id = 1", _a);
        Ok(db, "COMMIT", _a);

        // The row is gone from storage; _b reads it from the retained image of the commit it began before.
        Assert.Equal(10, Assert.Single(Ok(db, "SELECT V FROM T WHERE Id = 1", _b).Data)["V"]);
        Assert.Equal(new object?[] { 1, 2 }, Ids(Ok(db, "SELECT Id FROM T WHERE G = 7", _b)));
        Assert.True(db.Execute("UPDATE T SET V = 5 WHERE Id = 1", _b)[^1].IsError);
    }

    [Fact]
    public void VersionMetadata_IsReleasedWhenTheLastTransactionEnds()
    {
        using DataVoContext db = Seed(StorageMode.InMemory);
        Ok(db, "BEGIN", _a);
        for (int i = 0; i < 20; i++)
        {
            Ok(db, $"UPDATE T SET V = {100 + i} WHERE Id = 1", _b);
        }

        Ok(db, "SELECT * FROM T", _a);
        Assert.True(db.Engine.VersionStorageManager.VersionCount > 0);

        Ok(db, "COMMIT", _a);

        Assert.Equal(0, db.Engine.VersionStorageManager.VersionCount);
        Assert.False(db.Engine.VersionStorageManager.HasRetainedImages("Cos", "T"));
        Assert.Equal(0, db.Engine.CommitClock.InFlightCount);
    }

    [Fact]
    public void SnapshotReads_RecordNoVersionMetadata()
    {
        using DataVoContext db = Seed(StorageMode.InMemory);
        Ok(db, "BEGIN", _a);

        Ok(db, "SELECT * FROM T", _a);
        Ok(db, "SELECT V FROM T WHERE Id = 1", _a);
        Ok(db, "SELECT COUNT(*) FROM T WHERE G = 7", _a);

        Assert.Equal(0, db.Engine.VersionStorageManager.VersionCount);
    }

    [Fact]
    public void OlderSnapshotsKeepTheirVersions_UntilTheyEnd()
    {
        using DataVoContext db = Seed(StorageMode.InMemory);
        Ok(db, "BEGIN", _a);
        Ok(db, "UPDATE T SET V = 11 WHERE Id = 1", _c);
        Ok(db, "BEGIN", _b);
        Ok(db, "UPDATE T SET V = 12 WHERE Id = 1", _c);

        // _a started first, so the oldest snapshot still needs both images.
        Ok(db, "COMMIT", _b);
        Assert.Equal(10, Assert.Single(Ok(db, "SELECT V FROM T WHERE Id = 1", _a).Data)["V"]);

        Ok(db, "COMMIT", _a);
        Assert.False(db.Engine.VersionStorageManager.HasRetainedImages("Cos", "T"));
        Assert.Equal(12, Assert.Single(Ok(db, "SELECT V FROM T WHERE Id = 1", _c).Data)["V"]);
    }

    private static object?[] Ids(QueryResult result) =>
        [.. result.Data.Select(row => row["Id"]).OrderBy(id => (int)id!)];

    private DataVoContext Seed(StorageMode mode)
    {
        var db = new DataVoContext(new DataVoConfig
        {
            StorageMode = mode,
            DataDirectory = mode == StorageMode.InMemory ? null! : _directory,
            LsmStrictFsync = false,
        });
        Ok(db, "CREATE DATABASE Cos", _a);
        foreach (Guid session in new[] { _a, _b, _c })
        {
            Ok(db, "USE Cos", session);
        }

        Ok(db, "CREATE TABLE T (Id INT PRIMARY KEY, V INT, G INT)", _a);
        Ok(db, "CREATE INDEX ix_g ON T (G)", _a);
        Ok(db, "INSERT INTO T (Id, V, G) VALUES (1, 10, 7)", _a);
        Ok(db, "INSERT INTO T (Id, V, G) VALUES (2, 20, 7)", _a);
        return db;
    }

    private static QueryResult Ok(DataVoContext db, string sql, Guid session)
    {
        QueryResult result = db.Execute(sql, session)[^1];
        Assert.False(result.IsError, $"{sql} => {string.Join(" | ", result.Messages)}");
        return result;
    }
}
