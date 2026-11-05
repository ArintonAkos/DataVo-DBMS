using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Errors;

/// <summary>NOT NULL through ALTER TABLE ADD/MODIFY COLUMN, validated against existing rows.</summary>
public sealed class AlterNotNullTests
{
    [Fact]
    public void AddNotNullColumnWithDefault_FillsExistingRows_AndIsEnforced()
    {
        using DataVoContext db = Open(withRows: true);

        Assert.False(db.Execute("ALTER TABLE P ADD COLUMN Tag VARCHAR(10) NOT NULL DEFAULT 'none'").Single().IsError);

        Assert.All(db.Execute("SELECT Tag FROM P").Single().Data, row => Assert.Equal("none", row["Tag"]));
        Assert.Equal(DataVoErrorCodes.NotNullViolation,
            db.Execute("INSERT INTO P (Id, Name, Tag) VALUES (9, 'x', NULL)").Single().ErrorCode);
    }

    [Fact]
    public void AddNotNullColumnWithoutDefault_IsRejectedWhenRowsExist()
    {
        using DataVoContext db = Open(withRows: true);

        QueryResult result = db.Execute("ALTER TABLE P ADD COLUMN Tag VARCHAR(10) NOT NULL").Single();

        Assert.Equal(DataVoErrorCodes.NotNullViolation, result.ErrorCode);
        Assert.Contains(db.Execute("SELECT Tag FROM P"), r => r.IsError); // column was not added
    }

    [Fact]
    public void AddNotNullColumnWithoutDefault_IsAllowedOnEmptyTable()
    {
        using DataVoContext db = Open(withRows: false);

        Assert.False(db.Execute("ALTER TABLE P ADD COLUMN Tag VARCHAR(10) NOT NULL").Single().IsError);
        Assert.Equal(DataVoErrorCodes.NotNullViolation, db.Execute("INSERT INTO P (Id, Name) VALUES (1, 'a')").Single().ErrorCode);
    }

    [Fact]
    public void ModifyToNotNull_IsRejectedWhenNullsExist_AndAcceptedOtherwise()
    {
        using DataVoContext db = Open(withRows: true);
        db.Execute("INSERT INTO P (Id) VALUES (3)"); // Name is NULL

        QueryResult rejected = db.Execute("ALTER TABLE P MODIFY COLUMN Name VARCHAR(20) NOT NULL").Single();
        Assert.Equal(DataVoErrorCodes.NotNullViolation, rejected.ErrorCode);
        Assert.False(db.Execute("INSERT INTO P (Id) VALUES (4)").Single().IsError); // still nullable

        db.Execute("DELETE FROM P WHERE Name = NULL");
        db.Execute("UPDATE P SET Name = 'filled' WHERE Id = 3");
        db.Execute("UPDATE P SET Name = 'filled' WHERE Id = 4");
        Assert.False(db.Execute("ALTER TABLE P MODIFY COLUMN Name VARCHAR(20) NOT NULL").Single().IsError);
        Assert.Equal(DataVoErrorCodes.NotNullViolation, db.Execute("INSERT INTO P (Id) VALUES (5)").Single().ErrorCode);

        // MODIFY without NOT NULL redefines the column as nullable again.
        Assert.False(db.Execute("ALTER TABLE P MODIFY COLUMN Name VARCHAR(20)").Single().IsError);
        Assert.False(db.Execute("INSERT INTO P (Id) VALUES (6)").Single().IsError);
    }

    [Fact]
    public void AlteredNotNull_SurvivesRestart()
    {
        string path = TestStorageDirectories.Create("alter_not_null");
        using (DataVoContext first = Open(withRows: true, StorageMode.Lsm, path))
        {
            Assert.False(first.Execute("ALTER TABLE P ADD COLUMN Tag VARCHAR(10) NOT NULL DEFAULT 'none'").Single().IsError);
        }

        using var second = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = path, LsmStrictFsync = false });
        second.Execute("USE N");
        Assert.Equal(DataVoErrorCodes.NotNullViolation,
            second.Execute("INSERT INTO P (Id, Name, Tag) VALUES (9, 'x', NULL)").Single().ErrorCode);
    }

    private static DataVoContext Open(bool withRows, StorageMode mode = StorageMode.InMemory, string? path = null)
    {
        var db = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = path, LsmStrictFsync = false });
        db.Execute("CREATE DATABASE N");
        db.Execute("USE N");
        db.Execute("CREATE TABLE P (Id INT PRIMARY KEY, Name VARCHAR(20))");
        if (withRows)
        {
            db.Execute("INSERT INTO P (Id, Name) VALUES (1, 'a')");
            db.Execute("INSERT INTO P (Id, Name) VALUES (2, 'b')");
        }

        return db;
    }
}
