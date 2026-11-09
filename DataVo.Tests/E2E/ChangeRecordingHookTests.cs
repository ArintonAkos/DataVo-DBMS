using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Changes;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

/// <summary>The Recording hook must report each change before its commit reaches the durable log.</summary>
public sealed class ChangeRecordingHookTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_hook_{Guid.NewGuid():N}");
    private readonly List<string> _events = [];

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    [Theory]
    [InlineData("INSERT INTO T (Id, V) VALUES (2, 1)", "Insert")]
    [InlineData("UPDATE T SET V = 9 WHERE Id = 1", "Update")]
    [InlineData("DELETE FROM T WHERE Id = 1", "Delete")]
    public void Autocommit_ReportsTheChangeBeforeCaptured(string sql, string kind)
    {
        using DataVoContext db = Seed(StorageMode.InMemory);

        Ok(db, sql);

        Assert.Equal([$"recording:{kind}", "captured"], _events);
    }

    [Fact]
    public void ExplicitTransaction_ReportsAtCommitAndNotBefore()
    {
        using DataVoContext db = Seed(StorageMode.InMemory);
        Ok(db, "BEGIN");
        Ok(db, "UPDATE T SET V = 9 WHERE Id = 1");
        Assert.Empty(_events);

        Ok(db, "COMMIT");

        Assert.Equal(["recording:Update", "captured"], _events);
    }

    [Fact]
    public void RolledBackTransaction_ReportsNothing()
    {
        using DataVoContext db = Seed(StorageMode.InMemory);
        Ok(db, "BEGIN");
        Ok(db, "UPDATE T SET V = 9 WHERE Id = 1");
        Ok(db, "ROLLBACK");

        Assert.Empty(_events);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Lsm_ReportsBeforeTheCommitReachesTheLog(bool explicitTransaction)
    {
        using DataVoContext db = Seed(StorageMode.Lsm);
        long sizeAtHook = -1;
        db.Engine.Changes.Recording += (_, _) => sizeAtHook = CommitLogBytes();

        if (explicitTransaction)
        {
            Ok(db, "BEGIN");
            Ok(db, "UPDATE T SET V = 9 WHERE Id = 1");
            Ok(db, "COMMIT");
        }
        else
        {
            Ok(db, "UPDATE T SET V = 9 WHERE Id = 1");
        }

        Assert.True(sizeAtHook >= 0, "The hook did not run.");
        Assert.True(CommitLogBytes() > sizeAtHook, "The commit was already in the log when the hook ran.");
    }

    private long CommitLogBytes() =>
        Directory.GetFiles(_directory, "*", SearchOption.AllDirectories)
            .Where(path => Path.GetFileName(path).Contains("commit", StringComparison.OrdinalIgnoreCase)
                || Path.GetFileName(path).EndsWith(".wal", StringComparison.OrdinalIgnoreCase))
            .Sum(path => new FileInfo(path).Length);

    private DataVoContext Seed(StorageMode mode)
    {
        var db = new DataVoContext(new DataVoConfig
        {
            StorageMode = mode,
            DiskStoragePath = mode == StorageMode.InMemory ? null! : _directory,
        });
        Ok(db, "CREATE DATABASE H");
        Ok(db, "USE H");
        Ok(db, "CREATE TABLE T (Id INT PRIMARY KEY, V INT)");
        Ok(db, "INSERT INTO T (Id, V) VALUES (1, 0)");
        db.Engine.Changes.Enabled = true;
        db.Engine.Changes.Recording += (_, change) => _events.Add($"recording:{change.Kind}");
        db.Engine.Changes.Captured += _ => _events.Add("captured");
        return db;
    }

    private static void Ok(DataVoContext db, string sql)
    {
        QueryResult result = db.Execute(sql)[^1];
        Assert.False(result.IsError, $"{sql} => {string.Join(" | ", result.Messages)}");
    }
}
