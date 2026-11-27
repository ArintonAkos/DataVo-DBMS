using DataVo.Core;
using DataVo.Core.Runtime.Changes;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Reactive;

public class ChangeRecorderCloneTests
{
    [Fact]
    public void CapturedInsertImage_IsIndependent_CaseInsensitiveSnapshot()
    {
        using var ctx = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ctx.Execute("CREATE DATABASE CloneDb");
        ctx.Execute("USE CloneDb");
        ctx.Execute("CREATE TABLE T (Id INT, Name VARCHAR(20))");

        ChangeSet? captured = null;
        // Enable capture and record the published set.
        ctx.Changes.Enabled = true;
        ctx.Changes.Captured += set => captured = set;

        ctx.Execute("INSERT INTO T VALUES (1, 'alice')");

        Assert.NotNull(captured);
        RowChange change = Assert.Single(captured!.Changes);
        Assert.NotNull(change.After);

        // Case-insensitive key access (snapshot preserves OrdinalIgnoreCase).
        Assert.Equal(1, Convert.ToInt32(change.After!["id"]));
        Assert.Equal("alice", change.After!["NAME"]);

        // Independence: the captured insert image must be frozen — a later UPDATE to the same row
        // must not retroactively change the already-captured snapshot.
        ctx.Execute("UPDATE T SET Name = 'bob' WHERE Id = 1");
        Assert.Equal("alice", change.After!["NAME"]);
    }

    [Fact]
    public void CapturedUpdateAndDelete_CarryTypedImages_AndBuildDictionariesOnlyWhenRead()
    {
        using var ctx = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ctx.Execute("CREATE DATABASE TypedCaptureDb");
        ctx.Execute("USE TypedCaptureDb");
        ctx.Execute("CREATE TABLE T (Id INT PRIMARY KEY, Name VARCHAR(20))");
        ctx.Execute("INSERT INTO T VALUES (1, 'alice')");

        var captured = new List<RowChange>();
        ctx.Changes.Enabled = true;
        ctx.Changes.Captured += set => captured.AddRange(set.Changes);

        ctx.Execute("UPDATE T SET Name = 'bob' WHERE Id = 1");
        ctx.Execute("DELETE FROM T WHERE Id = 1");

        Assert.Equal(2, captured.Count);
        RowChange update = captured[0];
        RowChange delete = captured[1];
        Assert.Null(Field(update, "_before"));
        Assert.Null(Field(update, "_after"));
        Assert.Null(Field(delete, "_before"));

        Assert.Equal("alice", update.TypedBefore!.Value.AsRowRef()["Name"].AsString());
        Assert.Equal("bob", update.TypedAfter!.Value.AsRowRef()["Name"].AsString());
        Assert.Equal("bob", delete.TypedBefore!.Value.AsRowRef()["Name"].AsString());
        Assert.Null(delete.TypedAfter);

        // Owned readers still get case-insensitive dictionaries, built once and cached.
        Assert.Equal("alice", update.Before!["NAME"]);
        Assert.Equal(1, update.After!["id"]);
        Assert.Same(update.Before, update.Before);
        Assert.Null(delete.After);
    }

    private static object? Field(RowChange change, string name) =>
        typeof(RowChange)
            .GetField(name, System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .GetValue(change);
}
