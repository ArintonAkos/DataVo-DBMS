using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Errors;

/// <summary>
/// A transaction's buffered inserts are re-checked at COMMIT: a key another session committed in the
/// meantime makes the COMMIT fail instead of storing a duplicate primary key.
/// </summary>
public sealed class CommitConflictTests
{
    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Disk, StorageMode.Lsm };

    [Theory]
    [MemberData(nameof(Modes))]
    public void Commit_WithKeyTakenByAnotherSession_FailsAndAppliesNothing(StorageMode mode)
    {
        using var db = new DataVoContext(new DataVoConfig
        {
            StorageMode = mode,
            DiskStoragePath = mode == StorageMode.InMemory ? null : TestStorageDirectories.Create("commit_conflict"),
            LsmStrictFsync = false,
        });
        db.Execute("CREATE DATABASE C");
        Guid first = Guid.NewGuid();
        Guid second = Guid.NewGuid();
        db.Execute("USE C", first);
        db.Execute("USE C", second);
        db.Execute("CREATE TABLE T (Id INT PRIMARY KEY, Name VARCHAR(20))", first);

        db.Execute("BEGIN", first);
        db.Execute("INSERT INTO T (Id, Name) VALUES (50, 'a')", first);
        db.Execute("INSERT INTO T (Id, Name) VALUES (51, 'b')", first);
        Assert.False(db.Execute("INSERT INTO T (Id, Name) VALUES (51, 'other')", second).Single().IsError);

        QueryResult commit = db.Execute("COMMIT", first).Single();

        Assert.True(commit.IsError);
        Assert.Equal(DataVoErrorCodes.PrimaryKeyViolation, commit.ErrorCode);
        List<Dictionary<string, object?>> rows = db.Execute("SELECT * FROM T", second).Single().Data;
        Assert.Equal("other", Assert.Single(rows)["Name"]);
    }
}
