using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DML;

/// <summary>An open transaction must keep reading the data as it was at BEGIN, whatever others commit.</summary>
public sealed class SnapshotIsolationTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_si_{Guid.NewGuid():N}");
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
    public void UpdateAfterBegin_LeavesTheOldValueVisible(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _tx);
        Ok(db, "UPDATE T SET V = 5 WHERE Id = 1", _other);

        Assert.Equal(10, Assert.Single(Ok(db, "SELECT V FROM T WHERE Id = 1", _tx).Data)["V"]);
        Assert.Equal(2, Ok(db, "SELECT Id FROM T", _tx).Data.Count);

        Ok(db, "COMMIT", _tx);
        Assert.Equal(5, Assert.Single(Ok(db, "SELECT V FROM T WHERE Id = 1", _tx).Data)["V"]);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void DeleteAfterBegin_LeavesTheRowVisible(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _tx);
        Ok(db, "DELETE FROM T WHERE Id = 1", _other);

        Assert.Equal(10, Assert.Single(Ok(db, "SELECT V FROM T WHERE Id = 1", _tx).Data)["V"]);
        Assert.Equal(2, Ok(db, "SELECT Id FROM T", _tx).Data.Count);

        Ok(db, "ROLLBACK", _tx);
        Assert.Single(Ok(db, "SELECT Id FROM T", _tx).Data);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void InsertAfterBegin_StaysHidden(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _tx);
        Ok(db, "INSERT INTO T (Id, V, G) VALUES (3, 30, 1)", _other);

        Assert.Equal(2, Ok(db, "SELECT Id FROM T", _tx).Data.Count);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void SeveralUpdates_ReadTheVersionFromBegin(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "UPDATE T SET V = 11 WHERE Id = 1", _other);
        Ok(db, "BEGIN", _tx);
        Ok(db, "UPDATE T SET V = 12 WHERE Id = 1", _other);
        Ok(db, "UPDATE T SET V = 13 WHERE Id = 1", _other);

        Assert.Equal(11, Assert.Single(Ok(db, "SELECT V FROM T WHERE Id = 1", _tx).Data)["V"]);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void IndexedPredicate_MatchesTheValueFromBegin(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _tx);
        Ok(db, "UPDATE T SET G = 9 WHERE Id = 1", _other);

        Assert.Equal(2, Ok(db, "SELECT Id FROM T WHERE G = 7", _tx).Data.Count);
        Assert.Empty(Ok(db, "SELECT Id FROM T WHERE G = 9", _tx).Data);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Join_ReadsTheVersionFromBegin(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "CREATE TABLE U (Id INT PRIMARY KEY, Tid INT)", _other);
        Ok(db, "INSERT INTO U (Id, Tid) VALUES (1, 1)", _other);
        Ok(db, "BEGIN", _tx);
        Ok(db, "UPDATE T SET V = 5 WHERE Id = 1", _other);

        Dictionary<string, object?> row = Assert.Single(Ok(db, "SELECT t.V FROM U u JOIN T t ON u.Tid = t.Id", _tx).Data);
        Assert.Equal(10, row.Values.Single());
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void WriteToARowChangedSinceBegin_IsAConflict(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _tx);
        Ok(db, "UPDATE T SET V = 5 WHERE Id = 1", _other);

        Assert.True(db.Execute("UPDATE T SET V = 6 WHERE Id = 1", _tx)[^1].IsError);
        Ok(db, "ROLLBACK", _tx);

        Assert.Equal(5, Assert.Single(Ok(db, "SELECT V FROM T WHERE Id = 1", _other).Data)["V"]);
    }

    [Fact]
    public void RetainedImages_AreDroppedWhenTheLastTransactionEnds()
    {
        using DataVoContext db = Seed(StorageMode.InMemory);
        Ok(db, "BEGIN", _tx);
        Ok(db, "UPDATE T SET V = 5 WHERE Id = 1", _other);
        Assert.True(db.Engine.VersionStorageManager.HasRetainedImages("Si", "T"));

        Ok(db, "COMMIT", _tx);

        Assert.False(db.Engine.VersionStorageManager.HasRetainedImages("Si", "T"));
    }

    [Fact]
    public void NoOpenTransaction_RetainsNothing()
    {
        using DataVoContext db = Seed(StorageMode.InMemory);
        Ok(db, "UPDATE T SET V = 5 WHERE Id = 1", _other);

        Assert.False(db.Engine.VersionStorageManager.HasRetainedImages("Si", "T"));
    }

    [Fact]
    public void PreparedAndCompiledWrites_AreInvisibleToAnOpenSnapshot()
    {
        using DataVoContext db = Seed(StorageMode.Lsm);
        Ok(db, "BEGIN", _tx);

        DataVoPreparedStatement update = db.Prepare("UPDATE T SET V = @v WHERE Id = @id", _other);
        Assert.False(update.Execute(("v", 5), ("id", 1)).IsError);

        Assert.Equal(10, Assert.Single(Ok(db, "SELECT V FROM T WHERE Id = 1", _tx).Data)["V"]);
    }

    private DataVoContext Seed(StorageMode mode)
    {
        var db = new DataVoContext(new DataVoConfig
        {
            StorageMode = mode,
            DiskStoragePath = mode == StorageMode.InMemory ? null! : _directory,
        });
        Ok(db, "CREATE DATABASE Si", _tx);
        Ok(db, "USE Si", _tx);
        Ok(db, "USE Si", _other);
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
