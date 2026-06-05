using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public class ReactiveSubqueryTests
{
    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Disk)]
    public void In_Subquery_TracksInnerSet(StorageMode mode)
    {
        using var ctx = ChangeCaptureIntegrationTests.NewContext(mode, out _);
        ctx.Execute("CREATE TABLE R (Id INT PRIMARY KEY, Gid INT)");
        ctx.Execute("CREATE TABLE S (Gid INT PRIMARY KEY)");
        var live = new HashSet<int>();
        using var sub = ctx.Subscribe("SELECT Id FROM R WHERE Gid IN (SELECT Gid FROM S)", qc =>
        {
            foreach (var r in qc.Added) live.Add(Convert.ToInt32(r["Id"]));
            foreach (var r in qc.Removed) live.Remove(Convert.ToInt32(r["Id"]));
        });

        ctx.Execute("INSERT INTO R VALUES (1, 100)");
        ctx.Execute("INSERT INTO R VALUES (2, 200)");
        ctx.DispatchPendingNotifications();
        Assert.Empty(live);

        ctx.Execute("INSERT INTO S VALUES (100)");
        ctx.DispatchPendingNotifications();
        Assert.Equal(new[] { 1 }, live.OrderBy(x => x).ToArray());

        ctx.Execute("DELETE FROM S WHERE Gid=100");
        ctx.DispatchPendingNotifications();
        Assert.Empty(live);
    }
}
