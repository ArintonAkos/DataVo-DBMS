using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Errors;

/// <summary>
/// A COMMIT that fails leaves the database exactly as it was: no row, index entry or version metadata of the
/// transaction survives, whichever step rejected it — including a step that runs after other writes of the same
/// commit were already applied.
/// </summary>
public sealed class AtomicCommitTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_atomic_{Guid.NewGuid():N}");
    private readonly Guid _tx = Guid.NewGuid();
    private readonly Guid _other = Guid.NewGuid();
    private readonly Guid _reader = Guid.NewGuid();

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
    public void ForeignKeyFailureAfterDeletesAndUpdates_UndoesEverything(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);

        Ok(db, "BEGIN", _tx);
        Ok(db, "DELETE FROM Items WHERE Id = 1", _tx);
        Ok(db, "UPDATE Items SET Cat = 'z', Qty = 99 WHERE Id = 2", _tx);
        Ok(db, "INSERT INTO Items (Id, Cat, Qty) VALUES (5, 'n', 5)", _tx);
        Ok(db, "INSERT INTO Lines (Id, ParentId) VALUES (10, 3)", _tx);

        // Another session removes the parent the buffered child references: the commit's last phase rejects it.
        Ok(db, "DELETE FROM Parents WHERE Id = 3", _other);
        string before = Snapshot(db);

        QueryResult commit = db.Execute("COMMIT", _tx)[^1];

        Assert.True(commit.IsError, string.Join(" | ", commit.Messages));
        Assert.Equal(before, Snapshot(db));
        Assert.False(db.Engine.TransactionManager.HasActiveTransaction(_tx));

        // The same writes commit cleanly once the parent exists again.
        Ok(db, "INSERT INTO Parents (Id) VALUES (3)", _other);
        Ok(db, "BEGIN", _tx);
        Ok(db, "DELETE FROM Items WHERE Id = 1", _tx);
        Ok(db, "UPDATE Items SET Cat = 'z', Qty = 99 WHERE Id = 2", _tx);
        Ok(db, "INSERT INTO Lines (Id, ParentId) VALUES (10, 3)", _tx);
        Ok(db, "COMMIT", _tx);
        Assert.Equal(new object?[] { 2 }, Ids(Ok(db, "SELECT Id FROM Items WHERE Cat = 'z'", _other)));
        Assert.Empty(Ok(db, "SELECT Id FROM Items WHERE Cat = 'a'", _other).Data);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void FailedCommit_WithAnOpenSnapshot_LeavesTheSnapshotAndMetadataIntact(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        Ok(db, "BEGIN", _reader);
        string seenByReader = Snapshot(db, _reader);

        Ok(db, "BEGIN", _tx);
        Ok(db, "UPDATE Items SET Qty = 50 WHERE Id = 2", _tx);
        Ok(db, "DELETE FROM Items WHERE Id = 4", _tx);
        Ok(db, "INSERT INTO Lines (Id, ParentId) VALUES (10, 3)", _tx);
        Ok(db, "DELETE FROM Parents WHERE Id = 3", _other);
        int versionsBefore = db.Engine.VersionStorageManager.VersionCount;

        Assert.True(db.Execute("COMMIT", _tx)[^1].IsError);

        Assert.Equal(seenByReader, Snapshot(db, _reader));
        Assert.Equal(versionsBefore, db.Engine.VersionStorageManager.VersionCount);
        Ok(db, "UPDATE Items SET Qty = 51 WHERE Id = 2", _reader);
        Ok(db, "COMMIT", _reader);
        Assert.Equal(51, Assert.Single(Ok(db, "SELECT Qty FROM Items WHERE Id = 2", _other).Data)["Qty"]);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void UniqueValueTakenMeanwhile_RejectsTheUpdateAtCommit(StorageMode mode)
    {
        using DataVoContext db = Seed(mode);
        string before = Snapshot(db);

        Ok(db, "BEGIN", _tx);
        Ok(db, "UPDATE Items SET Code = 'K9' WHERE Id = 2", _tx);
        Ok(db, "INSERT INTO Items (Id, Cat, Qty) VALUES (6, 'n', 6)", _tx);
        Ok(db, "INSERT INTO Items (Id, Cat, Qty, Code) VALUES (7, 'n', 7, 'K9')", _other);
        string committedByOther = Snapshot(db);

        QueryResult commit = db.Execute("COMMIT", _tx)[^1];

        Assert.True(commit.IsError);
        Assert.NotEqual(before, committedByOther);
        Assert.Equal(committedByOther, Snapshot(db));
        Assert.Single(Ok(db, "SELECT Id FROM Items WHERE Code = 'K9'", _other).Data);
    }

    /// <summary>Everything a reader can observe about the tables: rows, index lookups and counts.</summary>
    private static string Snapshot(DataVoContext db, Guid? session = null)
    {
        Guid reader = session ?? Guid.Empty;
        if (session is null)
        {
            reader = Guid.NewGuid();
            Ok(db, "USE Atomic", reader);
        }

        var parts = new List<string>();
        foreach (string sql in new[]
        {
            "SELECT Id, Cat, Qty, Code FROM Items ORDER BY Id",
            "SELECT Id FROM Items WHERE Cat = 'a' ORDER BY Id",
            "SELECT Id FROM Items WHERE Cat = 'b' ORDER BY Id",
            "SELECT Id FROM Items WHERE Cat = 'z' ORDER BY Id",
            "SELECT Id FROM Items WHERE Code = 'K1' ORDER BY Id",
            "SELECT Cat, COUNT(*) AS C FROM Items GROUP BY Cat ORDER BY Cat",
            "SELECT Id, ParentId FROM Lines ORDER BY Id",
            "SELECT Id FROM Parents ORDER BY Id",
        })
        {
            parts.Add(string.Join(";", Ok(db, sql, reader).Data.Select(row => string.Join(",", row.Values))));
        }

        return string.Join(" / ", parts);
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
        Ok(db, "CREATE DATABASE Atomic", _tx);
        foreach (Guid session in new[] { _tx, _other, _reader })
        {
            Ok(db, "USE Atomic", session);
        }

        Ok(db, "CREATE TABLE Items (Id INT PRIMARY KEY, Cat VARCHAR(10), Qty INT, Code VARCHAR(10) UNIQUE)", _tx);
        Ok(db, "CREATE INDEX ix_cat ON Items (Cat)", _tx);
        Ok(db, "CREATE TABLE Parents (Id INT PRIMARY KEY)", _tx);
        Ok(db, "CREATE TABLE Lines (Id INT PRIMARY KEY, ParentId INT REFERENCES Parents(Id))", _tx);
        Ok(db, "INSERT INTO Items (Id, Cat, Qty, Code) VALUES (1, 'a', 1, 'K1')", _tx);
        Ok(db, "INSERT INTO Items (Id, Cat, Qty, Code) VALUES (2, 'a', 2, 'K2')", _tx);
        Ok(db, "INSERT INTO Items (Id, Cat, Qty, Code) VALUES (3, 'b', 3, NULL)", _tx);
        Ok(db, "INSERT INTO Items (Id, Cat, Qty, Code) VALUES (4, 'b', 4, NULL)", _tx);
        Ok(db, "INSERT INTO Parents (Id) VALUES (3)", _tx);
        return db;
    }

    private static QueryResult Ok(DataVoContext db, string sql, Guid session)
    {
        QueryResult result = db.Execute(sql, session)[^1];
        Assert.False(result.IsError, $"{sql} => {string.Join(" | ", result.Messages)}");
        return result;
    }
}
