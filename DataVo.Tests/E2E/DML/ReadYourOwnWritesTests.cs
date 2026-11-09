using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DML;

/// <summary>A SELECT inside a transaction must see that transaction's uncommitted writes, and nobody else may.</summary>
public sealed class ReadYourOwnWritesTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_ryow_{Guid.NewGuid():N}");
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
    public void OwnInsert_IsVisibleToItselfOnly(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _tx);
        Ok(db, "INSERT INTO T (Id, V, G) VALUES (3, 30, 7)", _tx);

        Assert.Equal(3, Ok(db, "SELECT Id FROM T", _tx).Data.Count);
        Assert.Equal(30, Assert.Single(Ok(db, "SELECT V FROM T WHERE Id = 3", _tx).Data)["V"]);
        Assert.Equal(2, Ok(db, "SELECT Id FROM T", _other).Data.Count);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void OwnUpdate_IsVisibleToItselfOnly(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _tx);
        Ok(db, "UPDATE T SET V = 99 WHERE Id = 1", _tx);

        Assert.Equal(99, Assert.Single(Ok(db, "SELECT V FROM T WHERE Id = 1", _tx).Data)["V"]);
        Assert.Equal(10, Assert.Single(Ok(db, "SELECT V FROM T WHERE Id = 1", _other).Data)["V"]);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void OwnDelete_HidesTheRowFromItselfOnly(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _tx);
        Ok(db, "DELETE FROM T WHERE Id = 1", _tx);

        Assert.Empty(Ok(db, "SELECT Id FROM T WHERE Id = 1", _tx).Data);
        Assert.Single(Ok(db, "SELECT Id FROM T WHERE Id = 1", _other).Data);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Rollback_DiscardsWhatTheTransactionSaw(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _tx);
        Ok(db, "INSERT INTO T (Id, V, G) VALUES (3, 30, 7)", _tx);
        Ok(db, "ROLLBACK", _tx);

        Assert.Equal(2, Ok(db, "SELECT Id FROM T", _tx).Data.Count);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Commit_KeepsWhatTheTransactionSaw(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _tx);
        Ok(db, "INSERT INTO T (Id, V, G) VALUES (3, 30, 7)", _tx);
        Ok(db, "UPDATE T SET V = 99 WHERE Id = 1", _tx);
        Ok(db, "COMMIT", _tx);

        Assert.Equal(3, Ok(db, "SELECT Id FROM T", _other).Data.Count);
        Assert.Equal(99, Assert.Single(Ok(db, "SELECT V FROM T WHERE Id = 1", _other).Data)["V"]);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void IndexedPredicate_SeesOwnWrites(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _tx);
        Ok(db, "INSERT INTO T (Id, V, G) VALUES (3, 30, 7)", _tx);
        Ok(db, "UPDATE T SET G = 9 WHERE Id = 1", _tx);

        Assert.Equal(new object?[] { 2, 3 }, Ids(Ok(db, "SELECT Id FROM T WHERE G = 7", _tx)));
        Assert.Equal(new object?[] { 1 }, Ids(Ok(db, "SELECT Id FROM T WHERE G = 9", _tx)));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void AggregatesOrderingAndLimits_SeeOwnWrites(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _tx);
        Ok(db, "INSERT INTO T (Id, V, G) VALUES (3, 30, 7)", _tx);

        Assert.Equal(3, Assert.Single(Ok(db, "SELECT COUNT(*) AS C FROM T", _tx).Data)["C"]);
        Assert.Equal(60, Convert.ToInt32(Assert.Single(Ok(db, "SELECT SUM(V) AS S FROM T", _tx).Data)["S"]));
        Assert.Equal(3, Assert.Single(Ok(db, "SELECT Id FROM T ORDER BY V DESC LIMIT 1", _tx).Data)["Id"]);
        Assert.Equal(3, Assert.Single(Ok(db, "SELECT G, COUNT(*) AS C FROM T GROUP BY G", _tx).Data)["C"]);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Join_And_Subquery_SeeOwnWrites(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "CREATE TABLE U (Id INT PRIMARY KEY, Tid INT)", _other);
        Ok(db, "BEGIN", _tx);
        Ok(db, "INSERT INTO U (Id, Tid) VALUES (1, 2)", _tx);
        Ok(db, "UPDATE T SET V = 77 WHERE Id = 2", _tx);

        Assert.Equal(77, Assert.Single(Ok(db, "SELECT t.V FROM U u JOIN T t ON u.Tid = t.Id", _tx).Data).Values.Single());
        Assert.Equal(new object?[] { 2 }, Ids(Ok(db, "SELECT Id FROM T WHERE Id IN (SELECT Tid FROM U)", _tx)));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void OmittedColumns_ReadAsTheirDefaults(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "CREATE TABLE D (Id INT PRIMARY KEY, Plain INT, Fallback INT DEFAULT 5)", _other);
        Ok(db, "BEGIN", _tx);
        Ok(db, "INSERT INTO D (Id) VALUES (1)", _tx);

        Dictionary<string, object?> row = Assert.Single(Ok(db, "SELECT * FROM D", _tx).Data);
        Assert.Null(row["Plain"]);
        Assert.Equal(5, row["Fallback"]);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void InsertThenDeleteOfOtherRow_CombineWithOthersCommits(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _tx);
        Ok(db, "UPDATE T SET V = 5 WHERE Id = 2", _other);
        Ok(db, "INSERT INTO T (Id, V, G) VALUES (3, 30, 7)", _tx);
        Ok(db, "DELETE FROM T WHERE Id = 1", _tx);

        List<Dictionary<string, object?>> rows = Ok(db, "SELECT Id, V FROM T", _tx).Data;
        Assert.Equal(new object?[] { 2, 3 }, Ids(Ok(db, "SELECT Id, V FROM T", _tx)));
        Assert.Equal(20, rows.Single(row => Equals(row["Id"], 2))["V"]);
    }

    [Fact]
    public void RollbackToSavepoint_RemovesLaterWritesFromView()
    {
        using DataVoContext db = Seed(StorageMode.InMemory);
        Ok(db, "BEGIN", _tx);
        Ok(db, "INSERT INTO T (Id, V, G) VALUES (3, 30, 7)", _tx);
        Ok(db, "SAVEPOINT s1", _tx);
        Ok(db, "INSERT INTO T (Id, V, G) VALUES (4, 40, 7)", _tx);
        Assert.Equal(4, Ok(db, "SELECT Id FROM T", _tx).Data.Count);

        Ok(db, "ROLLBACK TO SAVEPOINT s1", _tx);

        Assert.Equal(3, Ok(db, "SELECT Id FROM T", _tx).Data.Count);
    }

    [Fact]
    public void PreparedSelect_SeesOwnWrites()
    {
        using DataVoContext db = Seed(StorageMode.Lsm);
        Ok(db, "BEGIN", _tx);
        Ok(db, "UPDATE T SET V = 99 WHERE Id = 1", _tx);

        DataVoPreparedStatement select = db.Prepare("SELECT V FROM T WHERE Id = @id", _tx);

        Assert.Equal(99, Assert.Single(select.Execute(("id", 1)).Data)["V"]);
    }

    private static object?[] Ids(QueryResult result) =>
        [.. result.Data.Select(row => row["Id"]).OrderBy(id => (int)id!)];

    private DataVoContext Seed(StorageMode mode)
    {
        var db = new DataVoContext(new DataVoConfig
        {
            StorageMode = mode,
            DiskStoragePath = mode == StorageMode.InMemory ? null! : _directory,
        });
        Ok(db, "CREATE DATABASE R", _tx);
        Ok(db, "USE R", _tx);
        Ok(db, "USE R", _other);
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
