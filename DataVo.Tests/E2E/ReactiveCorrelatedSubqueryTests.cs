using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public class ReactiveCorrelatedSubqueryTests
{
    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Disk)]
    public void CorrelatedExists_TracksPerKey(StorageMode mode)
    {
        using var ctx = ChangeCaptureIntegrationTests.NewContext(mode, out _);
        ctx.Execute("CREATE TABLE R (Id INT PRIMARY KEY, K INT)");
        ctx.Execute("CREATE TABLE S (Id INT PRIMARY KEY, K INT)");
        var live = new HashSet<int>();
        using var sub = ctx.Subscribe(
            "SELECT Id FROM R WHERE EXISTS (SELECT 1 FROM S WHERE S.K = R.K)", qc =>
        {
            foreach (var r in qc.Added) live.Add(Convert.ToInt32(r["Id"]));
            foreach (var r in qc.Removed) live.Remove(Convert.ToInt32(r["Id"]));
        });

        ctx.Execute("INSERT INTO R VALUES (1, 100)");
        ctx.Execute("INSERT INTO R VALUES (2, 200)");
        ctx.DispatchPendingNotifications();
        Assert.Empty(live);

        ctx.Execute("INSERT INTO S VALUES (10, 100)"); // only R#1's key matches
        ctx.DispatchPendingNotifications();
        Assert.Equal(new[] { 1 }, live.OrderBy(x => x).ToArray());

        ctx.Execute("DELETE FROM S WHERE Id = 10");
        ctx.DispatchPendingNotifications();
        Assert.Empty(live);
    }
}
