using DataVo.Core;
using DataVo.Core.Runtime.Changes;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Reactive;

/// <summary>
/// <see cref="ChangeCapture.BeginOrigin"/> labels the change sets of the writes made inside it, without
/// changing what subscribers receive.
/// </summary>
public sealed class ChangeOriginTests : IDisposable
{
    private readonly DataVoContext _context = new(new DataVoConfig { StorageMode = StorageMode.InMemory });
    private readonly List<ChangeSet> _captured = [];

    public ChangeOriginTests()
    {
        Ok("CREATE DATABASE O");
        Ok("USE O");
        Ok("CREATE TABLE T (Id INT PRIMARY KEY, V INT)");
        _context.Engine.Changes.Enabled = true;
        _context.Engine.Changes.Captured += _captured.Add;
    }

    public void Dispose() => _context.Dispose();

    [Fact]
    public void ChangeSet_CarriesOrigin_OnlyInsideTheScope()
    {
        Ok("INSERT INTO T VALUES (1, 1)");
        using (ChangeCapture.BeginOrigin("import"))
        {
            Ok("INSERT INTO T VALUES (2, 2)");
        }

        Ok("INSERT INTO T VALUES (3, 3)");

        Assert.Equal([null, "import", null], _captured.Select(set => set.Origin));
    }

    [Fact]
    public void NestedScopes_RestoreTheOuterOrigin()
    {
        using (ChangeCapture.BeginOrigin("outer"))
        {
            using (ChangeCapture.BeginOrigin("inner"))
            {
                Assert.Equal("inner", ChangeCapture.CurrentOrigin);
                Ok("INSERT INTO T VALUES (1, 1)");
            }

            Assert.Equal("outer", ChangeCapture.CurrentOrigin);
            Ok("INSERT INTO T VALUES (2, 2)");
        }

        Assert.Null(ChangeCapture.CurrentOrigin);
        Assert.Equal(["inner", "outer"], _captured.Select(set => set.Origin));
    }

    [Fact]
    public void Commit_CarriesTheOriginInEffectAtCommit()
    {
        Ok("BEGIN TRANSACTION");
        Ok("INSERT INTO T VALUES (1, 1)");
        using (ChangeCapture.BeginOrigin("replica"))
        {
            Ok("COMMIT");
        }

        Assert.Equal("replica", Assert.Single(_captured).Origin);
    }

    [Fact]
    public void RecordingHandler_SeesTheOrigin_OnTheWritingThread()
    {
        var seen = new List<string?>();
        _context.Engine.Changes.Recording += (_, _) => seen.Add(ChangeCapture.CurrentOrigin);
        using (ChangeCapture.BeginOrigin("feed"))
        {
            Ok("INSERT INTO T VALUES (1, 1)");
        }

        Ok("INSERT INTO T VALUES (2, 2)");

        Assert.Equal(["feed", null], seen);
    }

    [Fact]
    public async Task Origin_FlowsIntoAsyncCalls()
    {
        using (ChangeCapture.BeginOrigin("async"))
        {
            await Task.Run(() => Ok("INSERT INTO T VALUES (1, 1)"));
        }

        Assert.Equal("async", Assert.Single(_captured).Origin);
    }

    [Fact]
    public void ReactiveSubscribers_StillReceiveTaggedChanges()
    {
        var added = new List<int>();
        using IDisposable subscription = _context.Subscribe("SELECT Id, V FROM T", change => added.AddRange(change.Added.Select(row => (int)row["Id"]!)));
        using (ChangeCapture.BeginOrigin("replica"))
        {
            Ok("INSERT INTO T VALUES (7, 7)");
        }

        _context.DispatchPendingNotifications();

        Assert.Equal([7], added);
    }

    private void Ok(string sql)
    {
        foreach (var result in _context.Execute(sql))
        {
            Assert.False(result.IsError, $"{sql}: {string.Join(" | ", result.Messages)}");
        }
    }
}
