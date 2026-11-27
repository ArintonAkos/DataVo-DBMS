using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Errors;

/// <summary>
/// A COMMIT whose buffered updates swap primary-key or UNIQUE values between rows must apply as one change, never
/// colliding with a row the same commit has not rewritten yet; and an UPDATE that points a foreign key at a parent
/// another session deleted meanwhile must be rejected at COMMIT, leaving nothing behind.
/// </summary>
public sealed class CommitKeySwapAndForeignKeyTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_swap_{Guid.NewGuid():N}");
    private readonly Guid _tx = Guid.NewGuid();
    private readonly Guid _other = Guid.NewGuid();

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
    public void TwoRowPrimaryKeySwap_CommitsBothRows(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _tx);
        Ok(db, "UPDATE T SET Id = 99 WHERE Id = 1", _tx);
        Ok(db, "UPDATE T SET Id = 1 WHERE Id = 2", _tx);
        Ok(db, "UPDATE T SET Id = 2 WHERE Id = 99", _tx);
        Assert.Equal("1:b:B;2:a:A;3:c:C", Rows(db, _tx));

        Ok(db, "COMMIT", _tx);

        AssertState(db, "1:b:B;2:a:A;3:c:C");
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void CyclicPrimaryKeyRotation_CommitsEveryRow(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _tx);
        Ok(db, "UPDATE T SET Id = 100 WHERE Id = 1", _tx);
        Ok(db, "UPDATE T SET Id = 1 WHERE Id = 3", _tx);
        Ok(db, "UPDATE T SET Id = 3 WHERE Id = 2", _tx);
        Ok(db, "UPDATE T SET Id = 2 WHERE Id = 100", _tx);

        Ok(db, "COMMIT", _tx);

        AssertState(db, "1:c:C;2:a:A;3:b:B");
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void UniqueValueSwap_CommitsBothRows(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _tx);
        Ok(db, "UPDATE T SET Code = 'tmp' WHERE Id = 1", _tx);
        Ok(db, "UPDATE T SET Code = 'a' WHERE Id = 2", _tx);
        Ok(db, "UPDATE T SET Code = 'b' WHERE Id = 1", _tx);

        Ok(db, "COMMIT", _tx);

        AssertState(db, "1:b:A;2:a:B;3:c:C");
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void CyclicUniqueRotation_CommitsEveryRow(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _tx);
        Ok(db, "UPDATE T SET Code = 'tmp' WHERE Id = 1", _tx);
        Ok(db, "UPDATE T SET Code = 'a' WHERE Id = 3", _tx);
        Ok(db, "UPDATE T SET Code = 'c' WHERE Id = 2", _tx);
        Ok(db, "UPDATE T SET Code = 'b' WHERE Id = 1", _tx);

        Ok(db, "COMMIT", _tx);

        AssertState(db, "1:b:A;2:c:B;3:a:C");
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void SwapCombinedWithDeleteAndInsert_Commits(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _tx);
        Ok(db, "DELETE FROM T WHERE Id = 3", _tx);
        Ok(db, "UPDATE T SET Id = 3, Code = 'z' WHERE Id = 1", _tx);
        Ok(db, "INSERT INTO T (Id, Code, Name) VALUES (1, 'a', 'N')", _tx);

        Ok(db, "COMMIT", _tx);

        AssertState(db, "1:a:N;2:b:B;3:z:A");
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void KeySwap_WithAnOlderSnapshotOpen_KeepsBothViews(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Guid reader = Guid.NewGuid();
        Ok(db, "USE Sw", reader);
        Ok(db, "BEGIN", reader);
        Assert.Equal("1:a:A;2:b:B;3:c:C", Rows(db, reader));

        Ok(db, "BEGIN", _tx);
        Ok(db, "UPDATE T SET Id = 99, Code = 'x' WHERE Id = 1", _tx);
        Ok(db, "UPDATE T SET Id = 1, Code = 'a' WHERE Id = 2", _tx);
        Ok(db, "UPDATE T SET Id = 2, Code = 'b' WHERE Id = 99", _tx);
        Ok(db, "COMMIT", _tx);

        // The older snapshot still reads the rows as they were, by scan and by key; a new one reads the swap.
        Assert.Equal("1:a:A;2:b:B;3:c:C", Rows(db, reader));
        Assert.Equal("1:a:A", Rows(db, reader, " WHERE Id = 1"));
        Assert.Equal("2:b:B", Rows(db, reader, " WHERE Id = 2"));
        Assert.Equal("1:a:B;2:b:A;3:c:C", Rows(db, _other));
        Ok(db, "COMMIT", reader);

        AssertState(db, "1:a:B;2:b:A;3:c:C");
        Assert.Equal(0, db.Engine.VersionStorageManager.VersionCount);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void UpdateOntoAParentDeletedMeanwhile_IsRejectedAtCommit(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Guid reader = Guid.NewGuid();
        Ok(db, "USE Sw", reader);
        Ok(db, "BEGIN", reader); // keeps the version metadata alive, so before/after compare exactly
        Ok(db, "BEGIN", _tx);
        Ok(db, "UPDATE Child SET ParentId = 1 WHERE Id = 20", _tx);
        Ok(db, "UPDATE T SET Name = 'changed' WHERE Id = 3", _tx);
        Ok(db, "DELETE FROM Parent WHERE Id = 1", _other);
        string before = State(db);
        int versions = db.Engine.VersionStorageManager.VersionCount;
        Assert.True(versions > 0);

        QueryResult commit = db.Execute("COMMIT", _tx)[^1];

        Assert.True(commit.IsError, "the commit kept a child pointing at a deleted parent");
        Assert.Contains(commit.Messages, message => message.Contains("foreign key", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(before, State(db));
        Assert.Equal(versions, db.Engine.VersionStorageManager.VersionCount);
        Assert.Equal(0, db.Engine.CommitClock.InFlightCount);
        Assert.False(db.Engine.TransactionManager.HasActiveTransaction(_tx));
        Assert.Equal(20, Assert.Single(Ok(db, "SELECT Id, ParentId FROM Child", reader).Data)["Id"]);
        Assert.Equal(2, Assert.Single(Ok(db, "SELECT ParentId FROM Child", reader).Data)["ParentId"]);
        Assert.Equal(2, Ok(db, "SELECT Id FROM Parent", reader).Data.Count); // the reader began before the delete
        Ok(db, "COMMIT", reader);
        Assert.Equal(0, db.Engine.VersionStorageManager.VersionCount);
        Assert.False(db.Engine.VersionStorageManager.HasRetainedImages("Sw", "Child"));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void UpdateOntoAParentInsertedInTheSameTransaction_Commits(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _tx);
        Ok(db, "INSERT INTO Parent (Id) VALUES (7)", _tx);
        Ok(db, "UPDATE Child SET ParentId = 7 WHERE Id = 20", _tx);

        Ok(db, "COMMIT", _tx);

        Assert.Equal(7, Assert.Single(Ok(db, "SELECT ParentId FROM Child WHERE Id = 20", _other).Data)["ParentId"]);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void UpdateOntoAParentTheSameTransactionDeletes_IsRejected(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _tx);
        Ok(db, "DELETE FROM Parent WHERE Id = 1", _tx);
        QueryResult update = db.Execute("UPDATE Child SET ParentId = 1 WHERE Id = 20", _tx)[^1];
        QueryResult commit = update.IsError ? update : db.Execute("COMMIT", _tx)[^1];

        Assert.True(commit.IsError, "a child was pointed at a parent the transaction deletes");
        db.Execute("ROLLBACK", _tx);
        Assert.Equal(2, Assert.Single(Ok(db, "SELECT ParentId FROM Child WHERE Id = 20", _other).Data)["ParentId"]);
    }

    private void AssertState(DataVoContext db, string expected)
    {
        Assert.Equal(expected, Rows(db, _other));
        string[] rows = expected.Split(';');
        foreach (string row in rows)
        {
            string[] parts = row.Split(':');
            Assert.Equal(row, Rows(db, _other, $" WHERE Id = {parts[0]}"));
            Assert.Equal(row, Rows(db, _other, $" WHERE Code = '{parts[1]}'"));
        }

        Assert.Equal((long)rows.Length, Assert.Single(Ok(db, "SELECT COUNT(*) AS C FROM T", _other).Data)["C"]);

        // The keys are free and taken exactly as the rows say.
        Assert.True(db.Execute("INSERT INTO T (Id, Code, Name) VALUES (1, 'q', 'Q')", _other)[^1].IsError);
        Assert.True(db.Execute($"INSERT INTO T (Id, Code, Name) VALUES (50, '{rows[0].Split(':')[1]}', 'Q')", _other)[^1].IsError);
    }

    private static string Rows(DataVoContext db, Guid session, string where = "") =>
        string.Join(";", Ok(db, $"SELECT Id, Code, Name FROM T{where}", session).Data
            .OrderBy(row => (int)row["Id"]!)
            .Select(row => $"{row["Id"]}:{row["Code"]}:{row["Name"]}"));

    private string State(DataVoContext db) =>
        Rows(db, _other) + " | " + string.Join(";", Ok(db, "SELECT Id, ParentId FROM Child", _other).Data
            .OrderBy(row => (int)row["Id"]!).Select(row => $"{row["Id"]}>{row["ParentId"]}"))
        + " | " + string.Join(";", Ok(db, "SELECT Id FROM Parent", _other).Data.Select(row => row["Id"]).OrderBy(id => (int)id!))
        + " | " + string.Join(";", Ok(db, "SELECT Id FROM Child WHERE ParentId = 2", _other).Data.Select(row => row["Id"]));

    private DataVoContext Seed(StorageMode mode)
    {
        var db = new DataVoContext(new DataVoConfig
        {
            StorageMode = mode,
            DataDirectory = mode == StorageMode.InMemory ? null! : _directory,
            LsmStrictFsync = false,
        });
        Ok(db, "CREATE DATABASE Sw", _tx);
        Ok(db, "USE Sw", _tx);
        Ok(db, "USE Sw", _other);
        Ok(db, "CREATE TABLE T (Id INT PRIMARY KEY, Code VARCHAR(10) UNIQUE, Name VARCHAR(10))", _tx);
        Ok(db, "INSERT INTO T (Id, Code, Name) VALUES (1, 'a', 'A'), (2, 'b', 'B'), (3, 'c', 'C')", _tx);
        Ok(db, "CREATE TABLE Parent (Id INT PRIMARY KEY)", _tx);
        Ok(db, "CREATE TABLE Child (Id INT PRIMARY KEY, ParentId INT REFERENCES Parent(Id))", _tx);
        Ok(db, "CREATE INDEX ix_child_parent ON Child (ParentId)", _tx);
        Ok(db, "INSERT INTO Parent (Id) VALUES (1), (2)", _tx);
        Ok(db, "INSERT INTO Child (Id, ParentId) VALUES (20, 2)", _tx);
        return db;
    }

    private static QueryResult Ok(DataVoContext db, string sql, Guid session)
    {
        QueryResult result = db.Execute(sql, session)[^1];
        Assert.False(result.IsError, $"{sql} => {string.Join(" | ", result.Messages)}");
        return result;
    }
}
