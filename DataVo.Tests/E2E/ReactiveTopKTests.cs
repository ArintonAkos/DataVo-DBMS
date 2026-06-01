using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public class ReactiveTopKTests
{
    private static int[] Ids(System.Collections.Generic.IEnumerable<System.Collections.Generic.IReadOnlyDictionary<string, object?>> rows)
        => rows.Select(r => Convert.ToInt32(r["Id"])).OrderBy(x => x).ToArray();

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Disk)]
    public void TopK_MaintainsWindow_OnInsertAndDelete(StorageMode mode)
    {
        using var ctx = ChangeCaptureIntegrationTests.NewContext(mode, out _);
        ctx.Execute("CREATE TABLE P (Id INT PRIMARY KEY, Score INT)");
        var live = new HashSet<int>();
        using var sub = ctx.Subscribe("SELECT Id, Score FROM P ORDER BY Score DESC LIMIT 2", qc =>
        {
            foreach (var r in qc.Added) live.Add(Convert.ToInt32(r["Id"]));
            foreach (var r in qc.Removed) live.Remove(Convert.ToInt32(r["Id"]));
        });

        ctx.Execute("INSERT INTO P VALUES (1, 10)");
        ctx.Execute("INSERT INTO P VALUES (2, 30)");
        ctx.Execute("INSERT INTO P VALUES (3, 20)");
        ctx.DispatchPendingNotifications();          // top-2 by score desc = {2(30), 3(20)}
        Assert.Equal(new[] { 2, 3 }, live.OrderBy(x => x).ToArray());

        ctx.Execute("DELETE FROM P WHERE Id = 2");   // removes top; 1(10) promoted
        ctx.DispatchPendingNotifications();          // now {3(20), 1(10)}
        Assert.Equal(new[] { 1, 3 }, live.OrderBy(x => x).ToArray());
    }
}
