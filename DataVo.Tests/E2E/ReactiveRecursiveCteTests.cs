using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public class ReactiveRecursiveCteTests
{
    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Disk)]
    public void Reachability_GrowsAndShrinks(StorageMode mode)
    {
        using var ctx = ChangeCaptureIntegrationTests.NewContext(mode, out _);
        ctx.Execute("CREATE TABLE Edge (Src INT, Dst INT, Id INT PRIMARY KEY)");
        const string sql = @"WITH RECURSIVE Reach AS (
            SELECT Src, Dst FROM Edge
            UNION ALL
            SELECT r.Src, e.Dst FROM Reach r INNER JOIN Edge e ON e.Src = r.Dst)
            SELECT Src, Dst FROM Reach";
        var live = new HashSet<(int, int)>();
        using var sub = ctx.Subscribe(sql, qc =>
        {
            foreach (var r in qc.Added) live.Add((Convert.ToInt32(r["Src"]), Convert.ToInt32(r["Dst"])));
            foreach (var r in qc.Removed) live.Remove((Convert.ToInt32(r["Src"]), Convert.ToInt32(r["Dst"])));
        });

        ctx.Execute("INSERT INTO Edge VALUES (1,2,1)");
        ctx.Execute("INSERT INTO Edge VALUES (2,3,2)"); // 1->2,2->3, and transitively 1->3
        ctx.DispatchPendingNotifications();
        Assert.Contains((1, 3), live);

        ctx.Execute("DELETE FROM Edge WHERE Id=2"); // removes 2->3, so 1->3 and 2->3 go away
        ctx.DispatchPendingNotifications();
        Assert.DoesNotContain((1, 3), live);
        Assert.DoesNotContain((2, 3), live);
        Assert.Contains((1, 2), live);
    }
}
