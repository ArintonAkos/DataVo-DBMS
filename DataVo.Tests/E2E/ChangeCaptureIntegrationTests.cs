using DataVo.Core;
using DataVo.Core.Runtime.Changes;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public class ChangeCaptureIntegrationTests
{
    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Disk)]
    public void Commit_ProducesChangeSet_RollbackDoesNot(StorageMode mode)
    {
        using var ctx = NewContext(mode, out _);
        ctx.Changes.Enabled = true;
        var captured = new List<ChangeSet>();
        ctx.Changes.Captured += captured.Add;

        ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50))");
        ctx.Execute("INSERT INTO Players VALUES (1, 'Ada')");

        Assert.Contains(captured, cs => cs.Changes.Any(c =>
            c.Table.Equals("Players", StringComparison.OrdinalIgnoreCase)
            && c.Kind == ChangeKind.Insert
            && Equals(c.After!["Id"], 1)));

        captured.Clear();
        ctx.Execute("BEGIN");
        ctx.Execute("INSERT INTO Players VALUES (2, 'Bob')");
        ctx.Execute("ROLLBACK");
        Assert.DoesNotContain(captured, cs => cs.Changes.Any(c => Equals(c.After?["Id"], 2)));
    }

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Disk)]
    public void Update_CapturesBeforeAndAfter(StorageMode mode)
    {
        using var ctx = NewContext(mode, out _);
        ctx.Changes.Enabled = true;
        var captured = new List<ChangeSet>();
        ctx.Changes.Captured += captured.Add;

        ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Lvl INT)");
        ctx.Execute("INSERT INTO Players VALUES (1, 5)");
        captured.Clear();
        ctx.Execute("UPDATE Players SET Lvl = 7 WHERE Id = 1");

        RowChange upd = captured.SelectMany(c => c.Changes)
            .Single(c => c.Kind == ChangeKind.Update);
        Assert.Equal(5, upd.Before!["Lvl"]);
        Assert.Equal(7, upd.After!["Lvl"]);
    }

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Disk)]
    public void BulkInsert_CapturesInserts(StorageMode mode)
    {
        using var ctx = NewContext(mode, out _);
        ctx.Changes.Enabled = true;
        var captured = new List<ChangeSet>();
        ctx.Changes.Captured += captured.Add;

        ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50))");
        ctx.BulkInsert("Players",
        [
            new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Ada" },
            new Dictionary<string, object?> { ["Id"] = 2, ["Name"] = "Bob" },
        ]);

        var inserts = captured.SelectMany(c => c.Changes)
            .Where(c => c.Kind == ChangeKind.Insert && c.Table.Equals("Players", StringComparison.OrdinalIgnoreCase))
            .ToList();
        Assert.Equal(2, inserts.Count);
        Assert.Contains(inserts, c => Equals(c.After!["Id"], 1));
        Assert.Contains(inserts, c => Equals(c.After!["Id"], 2));
    }

    internal static DataVoContext NewContext(StorageMode mode, out string db, string? path = null)
    {
        var ctx = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = path });
        db = $"Chg_{Guid.NewGuid():N}";
        ctx.Execute($"CREATE DATABASE {db}");
        ctx.Execute($"USE {db}");
        return ctx;
    }
}
