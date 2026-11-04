using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Exceptions;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Errors;

/// <summary>NOT NULL columns reject NULL on every write path, and the constraint survives a restart.</summary>
public sealed class NotNullTests
{
    private static readonly ReactiveRowSchema Schema = new("Id", "Name", "Note");

    [Theory]
    [InlineData("INSERT INTO P (Id, Note) VALUES (1, 'x')")]
    [InlineData("INSERT INTO P (Id, Name, Note) VALUES (1, NULL, 'x')")]
    [InlineData("INSERT INTO P (Id, Name) VALUES (1, 'ok'), (2, NULL)")]
    public void Insert_WithNullInNotNullColumn_IsRejected(string sql)
    {
        using DataVoContext db = Open();

        QueryResult result = db.Execute(sql).Single();

        Assert.True(result.IsError, string.Join(" / ", result.Messages));
        Assert.Equal(DataVoErrorCodes.NotNullViolation, result.ErrorCode);
        Assert.Contains("Name", result.Messages[0], StringComparison.Ordinal);
        Assert.Empty(db.Execute("SELECT * FROM P").Single().Data);
    }

    [Fact]
    public void NullableAndDefaultedColumns_KeepWorking()
    {
        using DataVoContext db = Open();

        Assert.False(db.Execute("INSERT INTO P (Id, Name) VALUES (1, 'a')").Single().IsError); // Note is nullable
        Assert.False(db.Execute("INSERT INTO P (Id, Name, Note) VALUES (2, 'b', NULL)").Single().IsError);
        Assert.False(db.Execute("INSERT INTO Q (Id) VALUES (1)").Single().IsError); // NOT NULL with DEFAULT
        Assert.Equal("anon", db.Execute("SELECT Name FROM Q").Single().Data.Single()["Name"]);
    }

    [Fact]
    public void Update_SettingNotNullColumnToNull_IsRejected()
    {
        using DataVoContext db = Open();
        db.Execute("INSERT INTO P (Id, Name) VALUES (1, 'a')");

        QueryResult result = db.Execute("UPDATE P SET Name = NULL WHERE Id = 1").Single();

        Assert.Equal(DataVoErrorCodes.NotNullViolation, result.ErrorCode);
        Assert.Equal("a", db.Execute("SELECT Name FROM P").Single().Data.Single()["Name"]);
    }

    [Fact]
    public void TypedInsert_WithNullInNotNullColumn_Throws()
    {
        using DataVoContext db = Open();

        var ex = Assert.Throws<ConstraintViolationException>(() =>
            db.InsertTyped("P", Schema, [CellValue.From(1), CellValue.Null, CellValue.From("x")]));

        Assert.Equal(DataVoErrorCodes.NotNullViolation, ex.ErrorCode);
    }

    [Fact]
    public void NotWithoutNull_IsASyntaxError()
    {
        using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        db.Execute("CREATE DATABASE N");
        db.Execute("USE N");

        QueryResult result = db.Execute("CREATE TABLE X (Id INT PRIMARY KEY, Name VARCHAR(10) NOT)").Single();

        Assert.Equal(DataVoErrorCodes.Syntax, result.ErrorCode);
    }

    [Fact]
    public void NotNull_SurvivesRestart()
    {
        string path = TestStorageDirectories.Create("not_null");
        using (DataVoContext first = Open(StorageMode.Lsm, path))
        {
        }

        using var second = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = path, LsmStrictFsync = false });
        second.Execute("USE N");

        Assert.Equal(DataVoErrorCodes.NotNullViolation, second.Execute("INSERT INTO P (Id) VALUES (1)").Single().ErrorCode);
    }

    private static DataVoContext Open(StorageMode mode = StorageMode.InMemory, string? path = null)
    {
        var db = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = path, LsmStrictFsync = false });
        foreach (string sql in new[]
        {
            "CREATE DATABASE N", "USE N",
            "CREATE TABLE P (Id INT PRIMARY KEY, Name VARCHAR(10) NOT NULL, Note VARCHAR(10) NULL)",
            "CREATE TABLE Q (Id INT PRIMARY KEY, Name VARCHAR(10) NOT NULL DEFAULT 'anon')",
        })
        {
            QueryResult? error = db.Execute(sql).FirstOrDefault(r => r.IsError);
            Assert.True(error is null, $"{sql}: {string.Join(" / ", error?.Messages ?? [])}");
        }

        return db;
    }
}
