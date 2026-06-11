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
}
